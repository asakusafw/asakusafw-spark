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
package com.asakusafw.spark.compiler
package subplan

import org.junit.runner.RunWith
import org.scalatest.FlatSpec
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
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import com.asakusafw.lang.compiler.model.description.{ ClassDescription, ImmediateDescription }
import com.asakusafw.lang.compiler.model.graph.{ Groups, MarkerOperator }
import com.asakusafw.lang.compiler.model.testing.OperatorExtractor
import com.asakusafw.lang.compiler.planning.{ PlanBuilder, PlanMarker }
import com.asakusafw.runtime.core.Result
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.runtime.value.{ BooleanOption, IntOption }
import com.asakusafw.spark.compiler.planning.{ SubPlanInfo, SubPlanOutputInfo }
import com.asakusafw.spark.compiler.spi.SubPlanCompiler
import com.asakusafw.spark.runtime.HadoopConfForEach
import com.asakusafw.spark.runtime.driver.{ BroadcastId, CoGroupDriver, ShuffleKey }
import com.asakusafw.spark.runtime.io.WritableSerDe
import com.asakusafw.spark.runtime.rdd.BranchKey
import com.asakusafw.spark.tools.asm._
import com.asakusafw.vocabulary.flow.processor.InputBuffer
import com.asakusafw.vocabulary.operator.CoGroup

@RunWith(classOf[JUnitRunner])
class CoGroupDriverClassBuilderSpecTest extends CoGroupDriverClassBuilderSpec

class CoGroupDriverClassBuilderSpec
  extends FlatSpec
  with SparkWithClassServerForAll
  with HadoopConfForEach
  with UsingCompilerContext {

  import CoGroupDriverClassBuilderSpec._

  behavior of classOf[CoGroupDriverClassBuilder].getSimpleName

  for {
    method <- Seq("cogroup", "cogroupEscape")
    (outputType, partitioners) <- Seq(
      (SubPlanOutputInfo.OutputType.DONT_CARE, 7),
      (SubPlanOutputInfo.OutputType.PREPARE_EXTERNAL_OUTPUT, 0))
  } {
    it should s"build cogroup driver class ${method} with OutputType.${outputType}" in {
      val foosMarker = MarkerOperator.builder(ClassDescription.of(classOf[Foo]))
        .attribute(classOf[PlanMarker], PlanMarker.GATHER).build()
      val barsMarker = MarkerOperator.builder(ClassDescription.of(classOf[Bar]))
        .attribute(classOf[PlanMarker], PlanMarker.GATHER).build()

      val operator = OperatorExtractor
        .extract(classOf[CoGroup], classOf[CoGroupOperator], method)
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

      implicit val context = newSubPlanCompilerContext(flowId, classServer.root.toFile)

      val compiler =
        SubPlanCompiler(
          subplan.getAttribute(classOf[SubPlanInfo]).getDriverType)
      val thisType = compiler.compile(subplan)
      context.addClass(context.branchKeys)
      context.addClass(context.broadcastIds)
      val cls = classServer.loadClass(thisType).asSubclass(classOf[CoGroupDriver])

      val fooOrd = new Foo.SortOrdering()
      val foos = sc.parallelize(0 until 10).map(Foo.intToFoo)
      val barOrd = new Bar.SortOrdering()
      val bars = sc.parallelize(0 until 10).flatMap(Bar.intToBars)
      val grouping = new GroupingOrdering()
      val part = new HashPartitioner(2)
      val driver = cls.getConstructor(
        classOf[SparkContext],
        classOf[Broadcast[Configuration]],
        classOf[Seq[(Seq[Future[RDD[(ShuffleKey, _)]]], Option[Ordering[ShuffleKey]])]],
        classOf[Ordering[ShuffleKey]],
        classOf[Partitioner],
        classOf[Map[BroadcastId, Broadcast[_]]])
        .newInstance(
          sc,
          hadoopConf,
          Seq((Seq(Future.successful(foos)), Option(fooOrd)), (Seq(Future.successful(bars)), Option(barOrd))),
          grouping,
          part,
          Map.empty)

      assert(driver.partitioners.size === partitioners)

      val branchKeyCls = classServer.loadClass(context.branchKeys.thisType.getClassName)
      def getBranchKey(marker: MarkerOperator): BranchKey = {
        val sn = subplan.getOperators.toSet
          .find(_.getOriginalSerialNumber == marker.getOriginalSerialNumber).get.getSerialNumber
        branchKeyCls.getField(context.branchKeys.getField(sn)).get(null).asInstanceOf[BranchKey]
      }

      assert(driver.branchKeys ===
        Set(fooResultMarker, barResultMarker,
          fooErrorMarker, barErrorMarker,
          fooAllMarker, barAllMarker,
          nResultMarker).map(getBranchKey))

      val results = driver.execute()
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
      assert(fooResult(0) === 1)

      assert(barResult.size === 1)
      assert(barResult(0)._1 === 10)
      assert(barResult(0)._2 === 1)

      assert(fooError.size === 9)
      assert(fooError(0) === 0)
      for (i <- 2 until 10) {
        assert(fooError(i - 1) === i)
      }

      assert(barError.size === 44)
      for {
        i <- 2 until 10
        j <- 0 until i
      } {
        assert(barError((i * (i - 1)) / 2 + j - 1)._1 == 10 + j)
        assert(barError((i * (i - 1)) / 2 + j - 1)._2 == i)
      }

      assert(fooAll.size === 10)
      for (i <- 0 until 10) {
        assert(fooAll(i) === i)
      }

      assert(barAll.size === 45)
      for {
        i <- 0 until 10
        j <- 0 until i
      } {
        assert(barAll((i * (i - 1)) / 2 + j)._1 == 10 + j)
        assert(barAll((i * (i - 1)) / 2 + j)._2 == i)
      }

      assert(nResult.size === 10)
      nResult.foreach(n => assert(n === 10))
    }
  }
}

object CoGroupDriverClassBuilderSpec {

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

    def intToFoo: Int => (ShuffleKey, Foo) = {

      lazy val foo = new Foo()

      { i =>
        foo.id.modify(i)
        val shuffleKey = new ShuffleKey(
          WritableSerDe.serialize(foo.id),
          WritableSerDe.serialize(new BooleanOption().modify(foo.id.get % 3 == 0)))
        (shuffleKey, foo)
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

    def intToBars: Int => Iterator[(ShuffleKey, Bar)] = {

      lazy val bar = new Bar()

      { i =>
        (0 until i).iterator.map { j =>
          bar.id.modify(10 + j)
          bar.fooId.modify(i)
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

    @CoGroup
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

    @CoGroup(inputBuffer = InputBuffer.ESCAPE)
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
