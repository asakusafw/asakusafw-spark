/*
 * Copyright 2011-2016 Asakusa Framework Team.
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
package graph

import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner

import java.io.{ DataInput, DataOutput }

import scala.collection.JavaConversions._
import scala.concurrent.{ Await, Future }
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.reflect.{ classTag, ClassTag }

import org.apache.hadoop.io.{ NullWritable, Writable }

import com.asakusafw.bridge.stage.StageInfo
import com.asakusafw.lang.compiler.model.description.{ ClassDescription, ImmediateDescription }
import com.asakusafw.lang.compiler.model.graph.{ Groups, MarkerOperator }
import com.asakusafw.lang.compiler.model.testing.OperatorExtractor
import com.asakusafw.lang.compiler.planning.SubPlan
import com.asakusafw.lang.compiler.planning.{ PlanBuilder, PlanMarker }
import com.asakusafw.runtime.core.Result
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.runtime.value.IntOption
import com.asakusafw.spark.compiler.{ ClassServerForAll, FlowIdForEach }
import com.asakusafw.spark.compiler.graph._
import com.asakusafw.spark.compiler.planning.{ IterativeInfo, SubPlanInfo, SubPlanOutputInfo }
import com.asakusafw.spark.runtime._
import com.asakusafw.spark.runtime.graph.{
  Broadcast,
  BroadcastId,
  Extract,
  ParallelCollectionSource,
  Source
}
import com.asakusafw.spark.runtime.rdd.{ BranchKey, ShuffleKey }
import com.asakusafw.vocabulary.operator.{ Extract => ExtractOp }

import com.asakusafw.spark.extensions.iterativebatch.compiler.spi.RoundAwareNodeCompiler
import com.asakusafw.spark.extensions.iterativebatch.runtime.graph.RoundAwareParallelCollectionSource

@RunWith(classOf[JUnitRunner])
class ExtractClassBuilderSpecTest extends ExtractClassBuilderSpec

class ExtractClassBuilderSpec
  extends FlatSpec
  with ClassServerForAll
  with SparkForAll
  with FlowIdForEach
  with UsingCompilerContext
  with JobContextSugar
  with RoundContextSugar {

  import ExtractClassBuilderSpec._

  behavior of classOf[ExtractClassBuilder].getSimpleName

  for {
    (outputType, partitioners) <- Seq(
      (SubPlanOutputInfo.OutputType.DONT_CARE, 3),
      (SubPlanOutputInfo.OutputType.PREPARE_EXTERNAL_OUTPUT, 1))
    iterativeInfo <- Seq(
      IterativeInfo.always(),
      IterativeInfo.never(),
      IterativeInfo.parameter("round"))
  } {
    val conf = s"OutputType: ${outputType}, IterativeInfo: ${iterativeInfo}"

    it should s"build extract class: [${conf}]" in {
      val foosMarker = MarkerOperator.builder(ClassDescription.of(classOf[Foo]))
        .attribute(classOf[PlanMarker], PlanMarker.CHECKPOINT).build()

      val operator = OperatorExtractor
        .extract(classOf[ExtractOp], classOf[ExtractOperator], "extract")
        .input("foos", ClassDescription.of(classOf[Foo]), foosMarker.getOutput)
        .output("fooResult", ClassDescription.of(classOf[Foo]))
        .output("barResult", ClassDescription.of(classOf[Bar]))
        .output("nResult", ClassDescription.of(classOf[N]))
        .argument("n", ImmediateDescription.of(10))
        .build()

      val fooResultMarker = MarkerOperator.builder(ClassDescription.of(classOf[Foo]))
        .attribute(classOf[PlanMarker], PlanMarker.CHECKPOINT).build()
      operator.findOutput("fooResult").connect(fooResultMarker.getInput)

      val barResultMarker = MarkerOperator.builder(ClassDescription.of(classOf[Bar]))
        .attribute(classOf[PlanMarker], PlanMarker.CHECKPOINT).build()
      operator.findOutput("barResult").connect(barResultMarker.getInput)

      val nResultMarker = MarkerOperator.builder(ClassDescription.of(classOf[N]))
        .attribute(classOf[PlanMarker], PlanMarker.CHECKPOINT).build()
      operator.findOutput("nResult").connect(nResultMarker.getInput)

      val plan = PlanBuilder.from(Seq(operator))
        .add(
          Seq(foosMarker),
          Seq(fooResultMarker, barResultMarker,
            nResultMarker)).build().getPlan()
      assert(plan.getElements.size === 1)

      val subplan = plan.getElements.head
      subplan.putAttr(
        new SubPlanInfo(_,
          SubPlanInfo.DriverType.EXTRACT,
          Seq.empty[SubPlanInfo.DriverOption],
          operator))
      subplan.putAttr(_ => iterativeInfo)

      val foosInput = subplan.findIn(foosMarker)

      subplan.findOut(fooResultMarker)
        .putAttr(
          new SubPlanOutputInfo(_,
            outputType,
            Seq.empty[SubPlanOutputInfo.OutputOption], null, null))

      subplan.findOut(barResultMarker)
        .putAttr(
          new SubPlanOutputInfo(_,
            SubPlanOutputInfo.OutputType.PARTITIONED,
            Seq.empty[SubPlanOutputInfo.OutputOption],
            Groups.parse(Seq("fooId"), Seq("-id")), null))

      subplan.findOut(nResultMarker)
        .putAttr(
          new SubPlanOutputInfo(_,
            outputType,
            Seq.empty[SubPlanOutputInfo.OutputOption], null, null))

      implicit val context = newNodeCompilerContext(flowId, classServer.root.toFile)
      context.branchKeys.getField(foosInput.getOperator.getSerialNumber)

      val compiler = RoundAwareNodeCompiler.get(subplan)
      val thisType = compiler.compile(subplan)
      context.addClass(context.branchKeys)
      context.addClass(context.broadcastIds)
      val cls = classServer.loadClass(thisType).asSubclass(classOf[Extract[_]])

      val branchKeyCls = classServer.loadClass(context.branchKeys.thisType.getClassName)
      def getBranchKey(marker: MarkerOperator): BranchKey = {
        val sn = subplan.getOperators.toSet
          .find(_.getOriginalSerialNumber == marker.getOriginalSerialNumber).get.getSerialNumber
        branchKeyCls.getField(context.branchKeys.getField(sn)).get(null).asInstanceOf[BranchKey]
      }

      implicit val jobContext = newJobContext(sc)

      val source =
        new RoundAwareParallelCollectionSource(getBranchKey(foosMarker), (0 until 10))("input")
          .mapWithRoundContext(getBranchKey(foosMarker))(Foo.intToFoo)
      val extract = cls.getConstructor(
        classOf[Seq[(Source, BranchKey)]],
        classOf[Map[BroadcastId, Broadcast[_]]],
        classOf[JobContext])
        .newInstance(
          Seq((source, getBranchKey(foosMarker))),
          Map.empty,
          jobContext)

      assert(extract.partitioners.size === partitioners)

      assert(extract.branchKeys ===
        Set(fooResultMarker, barResultMarker, nResultMarker).map(getBranchKey))

      for {
        round <- 0 to 1
      } {
        val rc = newRoundContext(
          stageId = s"round_${round}",
          batchArguments = Map("round" -> round.toString))
        val bias = if (iterativeInfo.isIterative) 100 * round else 0

        val results = extract.compute(rc)

        val ((fooResult, barResult), nResult) =
          Await.result(
            results(getBranchKey(fooResultMarker)).map {
              _().map {
                case (_, foo: Foo) => foo.id.get
              }.collect.toSeq
            }.zip {
              results(getBranchKey(barResultMarker)).map {
                _().map {
                  case (_, bar: Bar) => bar
                }.mapPartitionsWithIndex({
                  case (part, iter) => iter.map(bar => (part, bar.fooId.get, bar.id.get))
                }).collect.toSeq
              }
            }.zip {
              results(getBranchKey(nResultMarker)).map {
                _().map {
                  case (_, n: N) => n.n.get
                }.collect.toSeq
              }
            }, Duration.Inf)

        assert(fooResult.size === 10)
        assert(fooResult === (0 until 10).map(i => bias + i))

        assert(barResult.size === bias * 10 + 45)
        barResult.groupBy(_._2).foreach {
          case (fooId, bars) =>
            val part = bars.head._1
            assert(bars.tail.forall(_._1 == part))
            assert(bars.map(_._3) === (0 until fooId).map(j => (fooId * (fooId - 1)) / 2 + j).reverse)
        }

        assert(nResult.size === 10)
        nResult.foreach(n => assert(n === 10))
      }
    }
  }

  for {
    (outputType, partitioners) <- Seq(
      (SubPlanOutputInfo.OutputType.DONT_CARE, 1),
      (SubPlanOutputInfo.OutputType.PREPARE_EXTERNAL_OUTPUT, 0))
    iterativeInfo <- Seq(
      IterativeInfo.always(),
      IterativeInfo.never(),
      IterativeInfo.parameter("round"))
  } {
    val conf = s"OutputType: ${outputType}, IterativeInfo: ${iterativeInfo}"

    it should s"build extract class missing port connection: [${conf}]" in {
      val foosMarker = MarkerOperator.builder(ClassDescription.of(classOf[Foo]))
        .attribute(classOf[PlanMarker], PlanMarker.CHECKPOINT).build()

      val operator = OperatorExtractor
        .extract(classOf[ExtractOp], classOf[ExtractOperator], "extract")
        .input("foos", ClassDescription.of(classOf[Foo]), foosMarker.getOutput)
        .output("fooResult", ClassDescription.of(classOf[Foo]))
        .output("barResult", ClassDescription.of(classOf[Bar]))
        .output("nResult", ClassDescription.of(classOf[N]))
        .argument("n", ImmediateDescription.of(10))
        .build()

      val fooResultMarker = MarkerOperator.builder(ClassDescription.of(classOf[Foo]))
        .attribute(classOf[PlanMarker], PlanMarker.CHECKPOINT).build()
      operator.findOutput("fooResult").connect(fooResultMarker.getInput)

      val plan = PlanBuilder.from(Seq(operator))
        .add(
          Seq(foosMarker),
          Seq(fooResultMarker)).build().getPlan()
      assert(plan.getElements.size === 1)

      val subplan = plan.getElements.head
      subplan.putAttr(
        new SubPlanInfo(_,
          SubPlanInfo.DriverType.EXTRACT,
          Seq.empty[SubPlanInfo.DriverOption],
          operator))
      subplan.putAttr(_ => iterativeInfo)

      val foosInput = subplan.findIn(foosMarker)

      subplan.findOut(fooResultMarker)
        .putAttr(
          new SubPlanOutputInfo(_,
            outputType,
            Seq.empty[SubPlanOutputInfo.OutputOption], null, null))

      implicit val context = newNodeCompilerContext(flowId, classServer.root.toFile)
      context.branchKeys.getField(foosInput.getOperator.getSerialNumber)

      val compiler = RoundAwareNodeCompiler.get(subplan)
      val thisType = compiler.compile(subplan)
      context.addClass(context.branchKeys)
      context.addClass(context.broadcastIds)
      val cls = classServer.loadClass(thisType).asSubclass(classOf[Extract[_]])

      val branchKeyCls = classServer.loadClass(context.branchKeys.thisType.getClassName)
      def getBranchKey(marker: MarkerOperator): BranchKey = {
        val sn = subplan.getOperators.toSet
          .find(_.getOriginalSerialNumber == marker.getOriginalSerialNumber).get.getSerialNumber
        branchKeyCls.getField(context.branchKeys.getField(sn)).get(null).asInstanceOf[BranchKey]
      }

      implicit val jobContext = newJobContext(sc)

      val source =
        new RoundAwareParallelCollectionSource(getBranchKey(foosMarker), (0 until 10))("input")
          .mapWithRoundContext(getBranchKey(foosMarker))(Foo.intToFoo)
      val extract = cls.getConstructor(
        classOf[Seq[(Source, BranchKey)]],
        classOf[Map[BroadcastId, Broadcast[_]]],
        classOf[JobContext])
        .newInstance(
          Seq((source, getBranchKey(foosMarker))),
          Map.empty,
          jobContext)

      assert(extract.partitioners.size === partitioners)

      assert(extract.branchKeys ===
        Set(fooResultMarker).map(marker => getBranchKey(marker)))

      for {
        round <- 0 to 1
      } {
        val rc = newRoundContext(
          stageId = s"round_${round}",
          batchArguments = Map("round" -> round.toString))
        val bias = if (iterativeInfo.isIterative) 100 * round else 0

        val results = extract.compute(rc)

        val fooResult = Await.result(
          results(getBranchKey(fooResultMarker)).map {
            _().map {
              case (_, foo: Foo) => foo.id.get
            }.collect.toSeq
          }, Duration.Inf)

        assert(fooResult.size === 10)
        assert(fooResult === (0 until 10).map(i => bias + i))
      }
    }
  }
}

object ExtractClassBuilderSpec {

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

    def intToFoo(rc: RoundContext): Int => (_, Foo) = {

      val stageInfo = StageInfo.deserialize(rc.hadoopConf.value.get(StageInfo.KEY_NAME))
      val round = stageInfo.getBatchArguments()("round").toInt

      lazy val foo = new Foo()

      { i =>
        foo.id.modify(100 * round + i)
        (NullWritable.get, foo)
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

  class ExtractOperator {

    private val bar = new Bar()

    private val n = new N()

    @ExtractOp
    def extract(
      foo: Foo,
      fooResult: Result[Foo], barResult: Result[Bar],
      nResult: Result[N], n: Int): Unit = {
      fooResult.add(foo)
      for (i <- 0 until foo.id.get) {
        bar.reset()
        bar.id.modify((foo.id.get * (foo.id.get - 1)) / 2 + i)
        bar.fooId.copyFrom(foo.id)
        barResult.add(bar)
      }
      this.n.n.modify(n)
      nResult.add(this.n)
    }
  }
}
