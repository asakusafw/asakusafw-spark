/*
 * Copyright 2011-2015 Asakusa Framework Team.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.asakusafw.spark.extensions.iterativebatch.compiler
package flow

import org.junit.runner.RunWith
import org.scalatest.fixture.FlatSpec
import org.scalatest.junit.JUnitRunner

import java.io.{ DataInput, DataOutput }
import java.util.{ List => JList }

import scala.collection.JavaConversions._
import scala.concurrent.{ Await, Future }
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Writable
import org.apache.spark.{ HashPartitioner, Partitioner, SparkContext }
import org.apache.spark.rdd.RDD

import com.asakusafw.bridge.stage.StageInfo
import com.asakusafw.lang.compiler.model.description.{ ClassDescription, ImmediateDescription }
import com.asakusafw.lang.compiler.model.graph.{ Groups, MarkerOperator }
import com.asakusafw.lang.compiler.model.testing.OperatorExtractor
import com.asakusafw.lang.compiler.planning.{ PlanBuilder, PlanMarker }
import com.asakusafw.runtime.core.Result
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.runtime.value.{ BooleanOption, IntOption }
import com.asakusafw.spark.compiler.planning.{ SubPlanInfo, SubPlanOutputInfo }
import com.asakusafw.spark.compiler.subplan.`package`._
import com.asakusafw.spark.runtime.driver.{ BroadcastId, ShuffleKey }
import com.asakusafw.spark.runtime.io.WritableSerDe
import com.asakusafw.spark.runtime.rdd.BranchKey
import com.asakusafw.spark.tools.asm._
import com.asakusafw.vocabulary.flow.processor.InputBuffer
import com.asakusafw.vocabulary.operator.{ CoGroup => CoGroupOp }

import com.asakusafw.spark.extensions.iterativebatch.compiler.fixture.SparkWithClassServerForAll
import com.asakusafw.spark.extensions.iterativebatch.compiler.spi.NodeCompiler
import com.asakusafw.spark.extensions.iterativebatch.runtime.{ RoundContext, RoundContextSugar }
import com.asakusafw.spark.extensions.iterativebatch.runtime.flow.{
  Broadcast,
  CoGroup,
  GroupOrdering,
  ParallelCollectionSource,
  SortOrdering,
  Target
}

@RunWith(classOf[JUnitRunner])
class CoGroupClassBuilderSpecTest extends CoGroupClassBuilderSpec

class CoGroupClassBuilderSpec
  extends FlatSpec
  with SparkWithClassServerForAll
  with FlowIdForEach
  with UsingCompilerContext
  with RoundContextSugar {

  import CoGroupClassBuilderSpec._

  behavior of classOf[CoGroupClassBuilder].getSimpleName

  for {
    method <- Seq("cogroup", "cogroupEscape")
    (outputType, partitioners) <- Seq(
      (SubPlanOutputInfo.OutputType.DONT_CARE, 7),
      (SubPlanOutputInfo.OutputType.PREPARE_EXTERNAL_OUTPUT, 0))
  } {
    it should s"build cogroup class ${method} with OutputType.${outputType}" in { implicit sc =>
      val foosMarker = MarkerOperator.builder(ClassDescription.of(classOf[Foo]))
        .attribute(classOf[PlanMarker], PlanMarker.GATHER).build()
      val barsMarker = MarkerOperator.builder(ClassDescription.of(classOf[Bar]))
        .attribute(classOf[PlanMarker], PlanMarker.GATHER).build()

      val operator = OperatorExtractor
        .extract(classOf[CoGroupOp], classOf[CoGroupOperator], method)
        .input("foos", ClassDescription.of(classOf[Foo]),
          Groups.parse(Seq("id")),
          foosMarker.getOutput)
        .input("bars", ClassDescription.of(classOf[Bar]),
          Groups.parse(Seq("fooId"), Seq("+id")),
          barsMarker.getOutput)
        .output("fooResult", ClassDescription.of(classOf[Foo]))
        .output("barResult", ClassDescription.of(classOf[Bar]))
        .output("fooError", ClassDescription.of(classOf[Foo]))
        .output("barError", ClassDescription.of(classOf[Bar]))
        .output("nResult", ClassDescription.of(classOf[N]))
        .argument("n", ImmediateDescription.of(10))
        .build()

      val fooResultMarker = MarkerOperator.builder(ClassDescription.of(classOf[Foo]))
        .attribute(classOf[PlanMarker], PlanMarker.CHECKPOINT).build()
      operator.findOutput("fooResult").connect(fooResultMarker.getInput)

      val barResultMarker = MarkerOperator.builder(ClassDescription.of(classOf[Bar]))
        .attribute(classOf[PlanMarker], PlanMarker.CHECKPOINT).build()
      operator.findOutput("barResult").connect(barResultMarker.getInput)

      val fooErrorMarker = MarkerOperator.builder(ClassDescription.of(classOf[Foo]))
        .attribute(classOf[PlanMarker], PlanMarker.CHECKPOINT).build()
      operator.findOutput("fooError").connect(fooErrorMarker.getInput)

      val barErrorMarker = MarkerOperator.builder(ClassDescription.of(classOf[Bar]))
        .attribute(classOf[PlanMarker], PlanMarker.CHECKPOINT).build()
      operator.findOutput("barError").connect(barErrorMarker.getInput)

      val fooAllMarker = MarkerOperator.builder(ClassDescription.of(classOf[Foo]))
        .attribute(classOf[PlanMarker], PlanMarker.CHECKPOINT).build()
      operator.findOutput("fooResult").connect(fooAllMarker.getInput)
      operator.findOutput("fooError").connect(fooAllMarker.getInput)

      val barAllMarker = MarkerOperator.builder(ClassDescription.of(classOf[Bar]))
        .attribute(classOf[PlanMarker], PlanMarker.CHECKPOINT).build()
      operator.findOutput("barResult").connect(barAllMarker.getInput)
      operator.findOutput("barError").connect(barAllMarker.getInput)

      val nResultMarker = MarkerOperator.builder(ClassDescription.of(classOf[N]))
        .attribute(classOf[PlanMarker], PlanMarker.CHECKPOINT).build()
      operator.findOutput("nResult").connect(nResultMarker.getInput)

      val plan = PlanBuilder.from(Seq(operator))
        .add(
          Seq(foosMarker, barsMarker),
          Seq(fooResultMarker, barResultMarker,
            fooErrorMarker, barErrorMarker,
            fooAllMarker, barAllMarker,
            nResultMarker)).build().getPlan()
      assert(plan.getElements.size === 1)

      val subplan = plan.getElements.head
      subplan.putAttr(
        new SubPlanInfo(_,
          SubPlanInfo.DriverType.COGROUP,
          Seq.empty[SubPlanInfo.DriverOption],
          operator))

      val foosInput = subplan.findIn(foosMarker)
      val barsInput = subplan.findIn(barsMarker)

      for {
        marker <- Seq(
          fooResultMarker, barResultMarker,
          fooErrorMarker, barErrorMarker,
          fooAllMarker, barAllMarker,
          nResultMarker)
      } {
        subplan.findOut(marker)
          .putAttr(
            new SubPlanOutputInfo(_,
              outputType,
              Seq.empty[SubPlanOutputInfo.OutputOption], null, null))
      }

      implicit val context = newNodeCompilerContext(flowId, classServer.root.toFile)
      context.branchKeys.getField(foosInput.getOperator.getSerialNumber)
      context.branchKeys.getField(barsInput.getOperator.getSerialNumber)

      val compiler = NodeCompiler(subplan.getAttribute(classOf[SubPlanInfo]).getDriverType)
      val thisType = compiler.compile(subplan)
      context.addClass(context.branchKeys)
      context.addClass(context.broadcastIds)
      val cls = classServer.loadClass(thisType).asSubclass(classOf[CoGroup])

      val branchKeyCls = classServer.loadClass(context.branchKeys.thisType.getClassName)
      def getBranchKey(marker: MarkerOperator): BranchKey = {
        val sn = subplan.getOperators.toSet
          .find(_.getOriginalSerialNumber == marker.getOriginalSerialNumber).get.getSerialNumber
        branchKeyCls.getField(context.branchKeys.getField(sn)).get(null).asInstanceOf[BranchKey]
      }

      val foos =
        new ParallelCollectionSource(getBranchKey(foosMarker), (0 until 100))("foos")
          .mapWithRoundContext(getBranchKey(foosMarker))(Foo.intToFoo)
      val fooOrd = new Foo.SortOrdering()

      val bars =
        new ParallelCollectionSource(getBranchKey(barsMarker), (0 until 100))("bars")
          .flatMapWithRoundContext(getBranchKey(barsMarker)) {
            rc =>
              i: Int =>
                (0 until i).iterator.map(Bar.intToBar(rc)(i, _))
          }
      val barOrd = new Bar.SortOrdering()

      val grouping = new GroupingOrdering()
      val partitioner = new HashPartitioner(2)

      val cogroup = cls.getConstructor(
        classOf[Seq[(Seq[Target], Option[SortOrdering])]],
        classOf[GroupOrdering],
        classOf[Partitioner],
        classOf[Map[BroadcastId, Broadcast]],
        classOf[SparkContext])
        .newInstance(
          Seq(
            (Seq((foos, getBranchKey(foosMarker))), Option(fooOrd)),
            (Seq((bars, getBranchKey(barsMarker))), Option(barOrd))),
          grouping,
          partitioner,
          Map.empty,
          sc)

      assert(cogroup.partitioners.size === partitioners)

      assert(cogroup.branchKeys ===
        Set(fooResultMarker, barResultMarker,
          fooErrorMarker, barErrorMarker,
          fooAllMarker, barAllMarker,
          nResultMarker).map(getBranchKey))

      for {
        round <- 0 to 1
      } {
        val rc = newRoundContext(batchArguments = Map("round" -> round.toString))

        val results = cogroup.getOrCompute(rc)

        val (((fooResult, barResult), (fooError, barError)), ((fooAll, barAll), nResult)) =
          Await.result(
            results(getBranchKey(fooResultMarker)).map {
              _.map {
                case (_, foo: Foo) => foo.id.get
              }.collect.toSeq
            }.zip {
              results(getBranchKey(barResultMarker)).map {
                _.map {
                  case (_, bar: Bar) => (bar.id.get, bar.fooId.get)
                }.collect.toSeq
              }
            }.zip {
              results(getBranchKey(fooErrorMarker)).map {
                _.map {
                  case (_, foo: Foo) => foo.id.get
                }.collect.toSeq.sorted
              }.zip {
                results(getBranchKey(barErrorMarker)).map {
                  _.map {
                    case (_, bar: Bar) => (bar.id.get, bar.fooId.get)
                  }.collect.toSeq.sortBy(_._2)
                }
              }
            }.zip {
              results(getBranchKey(fooAllMarker)).map {
                _.map {
                  case (_, foo: Foo) => foo.id.get
                }.collect.toSeq.sorted
              }.zip {
                results(getBranchKey(barAllMarker)).map {
                  _.map {
                    case (_, bar: Bar) => (bar.id.get, bar.fooId.get)
                  }.collect.toSeq.sortBy(_._2)
                }
              }.zip {
                results(getBranchKey(nResultMarker)).map {
                  _.map {
                    case (_, n: N) => n.n.get
                  }.collect.toSeq
                }
              }
            }, Duration.Inf)

        assert(fooResult.size === 1)
        assert(fooResult.head === 100 * round + 1)

        assert(barResult.size === 1)
        assert(barResult.head._1 === 100 * round + 0)
        assert(barResult.head._2 === 100 * round + 1)

        assert(fooError.size === 99)
        assert(fooError.head === 100 * round + 0)
        for (i <- 2 until 10) {
          assert(fooError(i - 1) === 100 * round + i)
        }

        assert(barError.size === 4949)
        for {
          i <- 2 until 100
          j <- 0 until i
        } {
          assert(barError((i * (i - 1)) / 2 + j - 1)._1 == 100 * round + j)
          assert(barError((i * (i - 1)) / 2 + j - 1)._2 == 100 * round + i)
        }

        assert(fooAll.size === 100)
        for (i <- 0 until 100) {
          assert(fooAll(i) === 100 * round + i)
        }

        assert(barAll.size === 4950)
        for {
          i <- 0 until 100
          j <- 0 until i
        } {
          assert(barAll((i * (i - 1)) / 2 + j)._1 == 100 * round + +j)
          assert(barAll((i * (i - 1)) / 2 + j)._2 == 100 * round + i)
        }

        assert(nResult.size === 100)
        nResult.foreach(n => assert(n === 10))
      }
    }
  }
}

object CoGroupClassBuilderSpec {

  class GroupingOrdering extends Ordering[ShuffleKey] {

    override def compare(x: ShuffleKey, y: ShuffleKey): Int = {
      IntOption.compareBytes(x.grouping, 0, x.grouping.length, y.grouping, 0, y.grouping.length)
    }
  }

  class Foo extends DataModel[Foo] with Writable {

    val id = new IntOption()

    override def reset(): Unit = {
      id.setNull()
    }
    override def copyFrom(other: Foo): Unit = {
      id.copyFrom(other.id)
    }
    override def readFields(in: DataInput): Unit = {
      id.readFields(in)
    }
    override def write(out: DataOutput): Unit = {
      id.write(out)
    }

    def getIdOption: IntOption = id
  }

  object Foo {

    val intToFoo = { rc: RoundContext =>

      val stageInfo = StageInfo.deserialize(rc.hadoopConf.value.get(StageInfo.KEY_NAME))
      val round = stageInfo.getBatchArguments()("round").toInt

      new Function1[Int, (ShuffleKey, Foo)] with Serializable {

        @transient var f: Foo = _

        def foo: Foo = {
          if (f == null) {
            f = new Foo()
          }
          f
        }

        override def apply(i: Int): (ShuffleKey, Foo) = {
          foo.id.modify(100 * round + i)
          val shuffleKey = new ShuffleKey(
            WritableSerDe.serialize(foo.id),
            WritableSerDe.serialize(new BooleanOption().modify(i % 3 == 0)))
          (shuffleKey, foo)
        }
      }
    }

    class SortOrdering extends GroupingOrdering {

      override def compare(x: ShuffleKey, y: ShuffleKey): Int = {
        val cmp = super.compare(x, y)
        if (cmp == 0) {
          BooleanOption.compareBytes(x.ordering, 0, x.ordering.length, y.ordering, 0, y.ordering.length)
        } else {
          cmp
        }
      }
    }
  }

  class Bar extends DataModel[Bar] with Writable {

    val id = new IntOption()
    val fooId = new IntOption()

    override def reset(): Unit = {
      id.setNull()
      fooId.setNull()
    }
    override def copyFrom(other: Bar): Unit = {
      id.copyFrom(other.id)
      fooId.copyFrom(other.fooId)
    }
    override def readFields(in: DataInput): Unit = {
      id.readFields(in)
      fooId.readFields(in)
    }
    override def write(out: DataOutput): Unit = {
      id.write(out)
      fooId.write(out)
    }

    def getIdOption: IntOption = id
    def getFooIdOption: IntOption = fooId
  }

  object Bar {

    val intToBar = { rc: RoundContext =>

      val stageInfo = StageInfo.deserialize(rc.hadoopConf.value.get(StageInfo.KEY_NAME))
      val round = stageInfo.getBatchArguments()("round").toInt

      new Function2[Int, Int, (ShuffleKey, Bar)] with Serializable {

        @transient var b: Bar = _

        def bar: Bar = {
          if (b == null) {
            b = new Bar()
          }
          b
        }

        override def apply(i: Int, j: Int): (ShuffleKey, Bar) = {
          bar.id.modify(100 * round + j)
          bar.fooId.modify(100 * round + i)
          val shuffleKey = new ShuffleKey(
            WritableSerDe.serialize(bar.fooId),
            WritableSerDe.serialize(new IntOption().modify(bar.id.toString.hashCode)))
          (shuffleKey, bar)
        }
      }
    }

    class SortOrdering extends GroupingOrdering {

      override def compare(x: ShuffleKey, y: ShuffleKey): Int = {
        val cmp = super.compare(x, y)
        if (cmp == 0) {
          IntOption.compareBytes(x.ordering, 0, x.ordering.length, y.ordering, 0, y.ordering.length)
        } else {
          cmp
        }
      }
    }
  }

  class N extends DataModel[N] with Writable {

    val n = new IntOption()

    override def reset(): Unit = {
      n.setNull()
    }
    override def copyFrom(other: N): Unit = {
      n.copyFrom(other.n)
    }
    override def readFields(in: DataInput): Unit = {
      n.readFields(in)
    }
    override def write(out: DataOutput): Unit = {
      n.write(out)
    }

    def getNOption: IntOption = n
  }

  class CoGroupOperator {

    private[this] val n = new N

    @CoGroupOp
    def cogroup(
      foos: JList[Foo], bars: JList[Bar],
      fooResult: Result[Foo], barResult: Result[Bar],
      fooError: Result[Foo], barError: Result[Bar],
      nResult: Result[N], n: Int): Unit = {
      if (foos.size == 1 && bars.size == 1) {
        fooResult.add(foos(0))
        barResult.add(bars(0))
      } else {
        foos.foreach(fooError.add)
        bars.foreach(barError.add)
      }
      this.n.n.modify(n)
      nResult.add(this.n)
    }

    @CoGroupOp(inputBuffer = InputBuffer.ESCAPE)
    def cogroupEscape(
      foos: JList[Foo], bars: JList[Bar],
      fooResult: Result[Foo], barResult: Result[Bar],
      fooError: Result[Foo], barError: Result[Bar],
      nResult: Result[N], n: Int): Unit = {
      if (foos.size == 1 && bars.size == 1) {
        fooResult.add(foos(0))
        barResult.add(bars(0))
      } else {
        foos.foreach(fooError.add)
        bars.foreach(barError.add)
      }
      this.n.n.modify(n)
      nResult.add(this.n)
    }
  }
}
