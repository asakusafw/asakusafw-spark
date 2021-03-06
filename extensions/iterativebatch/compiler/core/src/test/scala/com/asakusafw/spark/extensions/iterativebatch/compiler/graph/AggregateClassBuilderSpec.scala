/*
 * Copyright 2011-2021 Asakusa Framework Team.
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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Writable
import org.apache.spark.{ HashPartitioner, Partitioner, SparkConf }
import org.apache.spark.rdd.RDD

import com.asakusafw.bridge.stage.StageInfo
import com.asakusafw.lang.compiler.model.description.{ ClassDescription, ImmediateDescription }
import com.asakusafw.lang.compiler.model.graph.{ Groups, MarkerOperator }
import com.asakusafw.lang.compiler.model.testing.OperatorExtractor
import com.asakusafw.lang.compiler.planning.{ PlanBuilder, PlanMarker }
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.runtime.value.IntOption
import com.asakusafw.spark.compiler.{ ClassServerForAll, FlowIdForEach }
import com.asakusafw.spark.compiler.graph._
import com.asakusafw.spark.compiler.planning.{
  IterativeInfo,
  PartitionGroupInfo,
  SubPlanInfo,
  SubPlanOutputInfo
}
import com.asakusafw.spark.runtime._
import com.asakusafw.spark.runtime.graph.{
  Broadcast,
  BroadcastId,
  Aggregate,
  ParallelCollectionSource,
  SortOrdering,
  Source
}
import com.asakusafw.spark.runtime.io.WritableSerDe
import com.asakusafw.spark.runtime.rdd.{ BranchKey, ShuffleKey }
import com.asakusafw.vocabulary.flow.processor.PartialAggregation
import com.asakusafw.vocabulary.operator.Fold

import com.asakusafw.spark.extensions.iterativebatch.compiler.spi.RoundAwareNodeCompiler
import com.asakusafw.spark.extensions.iterativebatch.runtime.graph.RoundAwareParallelCollectionSource

@RunWith(classOf[JUnitRunner])
class AggregateClassBuilderSpecTest extends AggregateClassBuilderSpec

class AggregateClassBuilderSpec
  extends FlatSpec
  with ClassServerForAll
  with SparkForAll
  with FlowIdForEach
  with UsingCompilerContext
  with JobContextSugar
  with RoundContextSugar {

  import AggregateClassBuilderSpec._

  behavior of classOf[AggregateClassBuilder].getSimpleName

  override def configure(conf: SparkConf): SparkConf = {
    conf.set(Props.Parallelism, 8.toString)
    super.configure(conf)
  }

  for {
    (dataSize, numPartitions) <- Seq(
      (PartitionGroupInfo.DataSize.TINY, 1),
      (PartitionGroupInfo.DataSize.SMALL, 4),
      (PartitionGroupInfo.DataSize.REGULAR, 8),
      (PartitionGroupInfo.DataSize.LARGE, 16),
      (PartitionGroupInfo.DataSize.HUGE, 32))
    iterativeInfo <- Seq(
      IterativeInfo.always(),
      IterativeInfo.never(),
      IterativeInfo.parameter("round"))
  } {
    val conf = s"DataSize: ${dataSize}, IterativeInfo: ${iterativeInfo}"

    it should s"build aggregate class: [${conf}]" in {
      val foosMarker = MarkerOperator.builder(ClassDescription.of(classOf[Foo]))
        .attribute(classOf[PlanMarker], PlanMarker.CHECKPOINT).build()

      val operator = OperatorExtractor
        .extract(classOf[Fold], classOf[FoldOperator], "fold")
        .input("foos", ClassDescription.of(classOf[Foo]), foosMarker.getOutput)
        .output("result", ClassDescription.of(classOf[Foo]))
        .argument("n", ImmediateDescription.of(10))
        .build()

      val resultMarker = MarkerOperator.builder(ClassDescription.of(classOf[Foo]))
        .attribute(classOf[PlanMarker], PlanMarker.CHECKPOINT).build()
      operator.findOutput("result").connect(resultMarker.getInput)

      val plan = PlanBuilder.from(Seq(operator))
        .add(
          Seq(foosMarker),
          Seq(resultMarker)).build().getPlan()
      assert(plan.getElements.size === 1)

      val subplan = plan.getElements.head
      subplan.putAttr(
        new SubPlanInfo(_,
          SubPlanInfo.DriverType.AGGREGATE,
          Seq.empty[SubPlanInfo.DriverOption],
          operator))
      subplan.putAttr(_ => iterativeInfo)

      val foosInput = subplan.findIn(foosMarker)

      subplan.findOut(resultMarker)
        .putAttr(
          new SubPlanOutputInfo(_,
            SubPlanOutputInfo.OutputType.AGGREGATED,
            Seq.empty[SubPlanOutputInfo.OutputOption],
            Groups.parse(Seq("i")),
            operator))
        .putAttr(_ => new PartitionGroupInfo(dataSize))

      implicit val context = newNodeCompilerContext(flowId, classServer.root.toFile)
      context.branchKeys.getField(foosInput.getOperator.getSerialNumber)

      val compiler = RoundAwareNodeCompiler.get(subplan)
      val thisType = compiler.compile(subplan)
      context.addClass(context.branchKeys)
      context.addClass(context.broadcastIds)
      val cls = classServer.loadClass(thisType).asSubclass(classOf[Aggregate[Foo, Foo]])

      val branchKeyCls = classServer.loadClass(context.branchKeys.thisType.getClassName)
      def getBranchKey(marker: MarkerOperator): BranchKey = {
        val sn = subplan.getOperators.toSet
          .find(_.getOriginalSerialNumber == marker.getOriginalSerialNumber).get.getSerialNumber
        branchKeyCls.getField(context.branchKeys.getField(sn)).get(null).asInstanceOf[BranchKey]
      }

      implicit val jobContext = newJobContext(sc)

      val foos =
        new RoundAwareParallelCollectionSource(getBranchKey(foosMarker), (0 until 10))("foos")
          .mapWithRoundContext(getBranchKey(foosMarker))(Foo.intToFoo)

      val aggregate = cls.getConstructor(
        classOf[Seq[(Source, BranchKey)]],
        classOf[Option[SortOrdering]],
        classOf[Partitioner],
        classOf[Map[BroadcastId, Broadcast[_]]],
        classOf[JobContext])
        .newInstance(
          Seq((foos, getBranchKey(foosMarker))),
          Option(new Foo.SortOrdering()),
          new HashPartitioner(2),
          Map.empty,
          jobContext)

      assert(aggregate.branchKeys === Set(resultMarker).map(getBranchKey))

      assert(aggregate.partitioners(getBranchKey(resultMarker)).get.numPartitions === numPartitions)

      for {
        round <- 0 to 1
      } {
        val rc = newRoundContext(
          stageId = s"round_${round}",
          batchArguments = Map("round" -> round.toString))
        val bias = if (iterativeInfo.isIterative) 100 * round else 0

        val results = aggregate.compute(rc)

        val result = Await.result(
          results(getBranchKey(resultMarker))
            .map { rddF =>
              val rdd = rddF()
              assert(rdd.partitions.size === numPartitions)
              rdd.map {
                case (_, foo: Foo) => (foo.i.get, foo.sum.get)
              }.collect.toSeq.sortBy(_._1)
            }, Duration.Inf)

        assert(result === Seq(
          (bias + 0, (0 until 10 by 2).map(i => bias + i * 100).sum + 4 * 10),
          (bias + 1, (1 until 10 by 2).map(i => bias + i * 100).sum + 4 * 10)))
      }
    }

    it should s"build aggregate class with grouping is empty: [${conf}]" in {
      val foosMarker = MarkerOperator.builder(ClassDescription.of(classOf[Foo]))
        .attribute(classOf[PlanMarker], PlanMarker.CHECKPOINT).build()

      val operator = OperatorExtractor
        .extract(classOf[Fold], classOf[FoldOperator], "fold")
        .input("foos", ClassDescription.of(classOf[Foo]), foosMarker.getOutput)
        .output("result", ClassDescription.of(classOf[Foo]))
        .argument("n", ImmediateDescription.of(10))
        .build()

      val resultMarker = MarkerOperator.builder(ClassDescription.of(classOf[Foo]))
        .attribute(classOf[PlanMarker], PlanMarker.CHECKPOINT).build()
      operator.findOutput("result").connect(resultMarker.getInput)

      val plan = PlanBuilder.from(Seq(operator))
        .add(
          Seq(foosMarker),
          Seq(resultMarker)).build().getPlan()
      assert(plan.getElements.size === 1)

      val subplan = plan.getElements.head
      subplan.putAttr(
        new SubPlanInfo(_,
          SubPlanInfo.DriverType.AGGREGATE,
          Seq.empty[SubPlanInfo.DriverOption],
          operator))
      subplan.putAttr(_ => iterativeInfo)

      val foosInput = subplan.findIn(foosMarker)

      subplan.findOut(resultMarker)
        .putAttr(
          new SubPlanOutputInfo(_,
            SubPlanOutputInfo.OutputType.AGGREGATED,
            Seq.empty[SubPlanOutputInfo.OutputOption],
            Groups.parse(Seq.empty[String]),
            operator))
        .putAttr(_ => new PartitionGroupInfo(dataSize))

      implicit val context = newNodeCompilerContext(flowId, classServer.root.toFile)
      context.branchKeys.getField(foosInput.getOperator.getSerialNumber)

      val compiler = RoundAwareNodeCompiler.get(subplan)
      val thisType = compiler.compile(subplan)
      context.addClass(context.branchKeys)
      context.addClass(context.broadcastIds)
      val cls = classServer.loadClass(thisType).asSubclass(classOf[Aggregate[Foo, Foo]])

      val branchKeyCls = classServer.loadClass(context.branchKeys.thisType.getClassName)
      def getBranchKey(marker: MarkerOperator): BranchKey = {
        val sn = subplan.getOperators.toSet
          .find(_.getOriginalSerialNumber == marker.getOriginalSerialNumber).get.getSerialNumber
        branchKeyCls.getField(context.branchKeys.getField(sn)).get(null).asInstanceOf[BranchKey]
      }

      implicit val jobContext = newJobContext(sc)

      val foos =
        new RoundAwareParallelCollectionSource(getBranchKey(foosMarker), (0 until 10))("foos")
          .mapWithRoundContext(getBranchKey(foosMarker))(Foo.intToFoo)
          .map(getBranchKey(foosMarker)) {
            foo: (ShuffleKey, Foo) =>
              (new ShuffleKey(Array.emptyByteArray, Array.emptyByteArray), foo._2)
          }

      val aggregate = cls.getConstructor(
        classOf[Seq[(Source, BranchKey)]],
        classOf[Option[SortOrdering]],
        classOf[Partitioner],
        classOf[Map[BroadcastId, Broadcast[_]]],
        classOf[JobContext])
        .newInstance(
          Seq((foos, getBranchKey(foosMarker))),
          None,
          new HashPartitioner(2),
          Map.empty,
          jobContext)

      assert(aggregate.branchKeys === Set(resultMarker).map(getBranchKey))

      assert(aggregate.partitioners(getBranchKey(resultMarker)).get.numPartitions === 1)

      for {
        round <- 0 to 1
      } {
        val rc = newRoundContext(
          stageId = s"round_${round}",
          batchArguments = Map("round" -> round.toString))
        val bias = if (iterativeInfo.isIterative) 100 * round else 0

        val results = aggregate.compute(rc)

        val result = Await.result(
          results(getBranchKey(resultMarker))
            .map { rddF =>
              val rdd = rddF()
              assert(rdd.partitions.size === 1)
              rdd.map {
                case (_, foo: Foo) => (foo.i.get, foo.sum.get)
              }.collect.toSeq.sortBy(_._1)
            }, Duration.Inf)

        assert(result.size === 1)
        assert(result(0)._2 === (0 until 10).map(i => bias + i * 100).sum + 9 * 10)
      }
    }
  }
}

object AggregateClassBuilderSpec {

  class Foo extends DataModel[Foo] with Writable {

    val i = new IntOption()
    val sum = new IntOption()

    override def reset(): Unit = {
      i.setNull()
      sum.setNull()
    }
    override def copyFrom(other: Foo): Unit = {
      i.copyFrom(other.i)
      sum.copyFrom(other.sum)
    }
    override def readFields(in: DataInput): Unit = {
      i.readFields(in)
      sum.readFields(in)
    }
    override def write(out: DataOutput): Unit = {
      i.write(out)
      sum.write(out)
    }

    def getIOption: IntOption = i
    def getSumOption: IntOption = sum
  }

  object Foo {

    def intToFoo(rc: RoundContext): Int => (_, Foo) = {

      val stageInfo = StageInfo.deserialize(rc.hadoopConf.value.get(StageInfo.KEY_NAME))
      val round = stageInfo.getBatchArguments()("round").toInt

      lazy val foo = new Foo()

      { i =>
        foo.i.modify(100 * round + (i % 2))
        foo.sum.modify(100 * round + i * 100)
        val shuffleKey = new ShuffleKey(
          WritableSerDe.serialize(foo.i),
          Array.emptyByteArray)
        (shuffleKey, foo)
      }
    }

    class SortOrdering extends Ordering[ShuffleKey] {

      override def compare(x: ShuffleKey, y: ShuffleKey): Int = {
        IntOption.compareBytes(x.grouping, 0, x.grouping.length, y.grouping, 0, y.grouping.length)
      }
    }
  }

  class FoldOperator {

    @Fold(partialAggregation = PartialAggregation.PARTIAL)
    def fold(acc: Foo, value: Foo, n: Int): Unit = {
      acc.sum.add(value.sum)
      acc.sum.add(n)
    }
  }
}
