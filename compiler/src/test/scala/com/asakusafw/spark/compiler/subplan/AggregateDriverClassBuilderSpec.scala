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

import scala.collection.JavaConversions._
import scala.concurrent.{ Await, Future }
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Writable
import org.apache.spark.{ HashPartitioner, Partitioner, SparkConf, SparkContext }
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import com.asakusafw.lang.compiler.model.description.{ ClassDescription, ImmediateDescription }
import com.asakusafw.lang.compiler.model.graph.{ Groups, MarkerOperator }
import com.asakusafw.lang.compiler.model.testing.OperatorExtractor
import com.asakusafw.lang.compiler.planning.{ PlanBuilder, PlanMarker }
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.runtime.value.IntOption
import com.asakusafw.spark.compiler.planning.{ PartitionGroupInfo, SubPlanInfo, SubPlanOutputInfo }
import com.asakusafw.spark.compiler.spi.SubPlanCompiler
import com.asakusafw.spark.runtime.{ HadoopConfForEach, Props }
import com.asakusafw.spark.runtime.driver.{ AggregateDriver, BroadcastId, ShuffleKey }
import com.asakusafw.spark.runtime.io.WritableSerDe
import com.asakusafw.spark.runtime.rdd.BranchKey
import com.asakusafw.vocabulary.flow.processor.PartialAggregation
import com.asakusafw.vocabulary.operator.Fold

@RunWith(classOf[JUnitRunner])
class AggregateDriverClassBuilderSpecTest extends AggregateDriverClassBuilderSpec

class AggregateDriverClassBuilderSpec
  extends FlatSpec
  with SparkWithClassServerForAll
  with HadoopConfForEach
  with UsingCompilerContext {

  import AggregateDriverClassBuilderSpec._

  behavior of classOf[AggregateDriverClassBuilder].getSimpleName

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
  } {
    it should s"build aggregate driver class with DataSize.${dataSize}" in {
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
      subplan.putAttribute(classOf[SubPlanInfo],
        new SubPlanInfo(subplan, SubPlanInfo.DriverType.AGGREGATE, Seq.empty[SubPlanInfo.DriverOption], operator))
      val subplanOutput = subplan.getOutputs.find(_.getOperator.getOriginalSerialNumber == resultMarker.getOriginalSerialNumber).get
      subplanOutput.putAttribute(classOf[SubPlanOutputInfo],
        new SubPlanOutputInfo(subplanOutput, SubPlanOutputInfo.OutputType.AGGREGATED, Seq.empty[SubPlanOutputInfo.OutputOption], Groups.parse(Seq("i")), operator))
      subplanOutput.putAttribute(classOf[PartitionGroupInfo], new PartitionGroupInfo(dataSize))

      implicit val context = newSubPlanCompilerContext(flowId, classServer.root.toFile)

      val compiler = SubPlanCompiler(subplan.getAttribute(classOf[SubPlanInfo]).getDriverType)
      val thisType = compiler.compile(subplan)
      context.addClass(context.branchKeys)
      context.addClass(context.broadcastIds)
      val cls = classServer.loadClass(thisType).asSubclass(classOf[AggregateDriver[Foo, Foo]])

      val foos = sc.parallelize(0 until 10).map { i =>
        val foo = new Foo()
        foo.i.modify(i % 2)
        foo.sum.modify(i)
        val serde = new WritableSerDe()
        (new ShuffleKey(serde.serialize(foo.i), Array.empty), foo)
      }
      val driver = cls.getConstructor(
        classOf[SparkContext],
        classOf[Broadcast[Configuration]],
        classOf[Seq[Future[RDD[(ShuffleKey, _)]]]],
        classOf[Option[Ordering[ShuffleKey]]],
        classOf[Partitioner],
        classOf[Map[BroadcastId, Broadcast[_]]])
        .newInstance(
          sc,
          hadoopConf,
          Seq(Future.successful(foos)),
          Option(new SortOrdering()),
          new HashPartitioner(2),
          Map.empty)

      val results = driver.execute()

      val branchKeyCls = classServer.loadClass(context.branchKeys.thisType.getClassName)
      def getBranchKey(osn: Long): BranchKey = {
        val sn = subplan.getOperators.toSet.find(_.getOriginalSerialNumber == osn).get.getSerialNumber
        branchKeyCls.getField(context.branchKeys.getField(sn)).get(null).asInstanceOf[BranchKey]
      }

      assert(driver.branchKeys ===
        Set(resultMarker)
        .map(marker => getBranchKey(marker.getOriginalSerialNumber)))

      assert(driver.partitioners(getBranchKey(resultMarker.getOriginalSerialNumber)).get.numPartitions === numPartitions)

      val result = Await.result(
        results(getBranchKey(resultMarker.getOriginalSerialNumber))
          .map { rdd =>
            assert(rdd.partitions.size === numPartitions)
            rdd.map {
              case (_, foo: Foo) => (foo.i.get, foo.sum.get)
            }.collect.toSeq.sortBy(_._1)
          }, Duration.Inf)

      assert(result.size === 2)
      assert(result(0)._1 === 0)
      assert(result(0)._2 === (0 until 10 by 2).sum + 4 * 10)
      assert(result(1)._1 === 1)
      assert(result(1)._2 === (1 until 10 by 2).sum + 4 * 10)
    }

    it should s"build aggregate driver class with DataSize.${dataSize} with grouping is empty" in {
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
      subplan.putAttribute(classOf[SubPlanInfo],
        new SubPlanInfo(subplan, SubPlanInfo.DriverType.AGGREGATE, Seq.empty[SubPlanInfo.DriverOption], operator))
      val subplanOutput = subplan.getOutputs.find(_.getOperator.getOriginalSerialNumber == resultMarker.getOriginalSerialNumber).get
      subplanOutput.putAttribute(classOf[SubPlanOutputInfo],
        new SubPlanOutputInfo(subplanOutput, SubPlanOutputInfo.OutputType.AGGREGATED, Seq.empty[SubPlanOutputInfo.OutputOption], Groups.parse(Seq.empty[String]), operator))
      subplanOutput.putAttribute(classOf[PartitionGroupInfo], new PartitionGroupInfo(dataSize))

      implicit val context = newSubPlanCompilerContext(flowId, classServer.root.toFile)

      val compiler = SubPlanCompiler(subplan.getAttribute(classOf[SubPlanInfo]).getDriverType)
      val thisType = compiler.compile(subplan)
      context.addClass(context.branchKeys)
      context.addClass(context.broadcastIds)
      val cls = classServer.loadClass(thisType).asSubclass(classOf[AggregateDriver[Foo, Foo]])

      val foos = sc.parallelize(0 until 10).map { i =>
        val foo = new Foo()
        foo.i.modify(i % 2)
        foo.sum.modify(i)
        val serde = new WritableSerDe()
        (new ShuffleKey(serde.serialize(foo.i), Array.empty), foo)
      }
      val driver = cls.getConstructor(
        classOf[SparkContext],
        classOf[Broadcast[Configuration]],
        classOf[Seq[Future[RDD[(ShuffleKey, _)]]]],
        classOf[Option[Ordering[ShuffleKey]]],
        classOf[Partitioner],
        classOf[Map[BroadcastId, Broadcast[_]]])
        .newInstance(
          sc,
          hadoopConf,
          Seq(Future.successful(foos)),
          Option(new SortOrdering()),
          new HashPartitioner(2),
          Map.empty)

      val results = driver.execute()

      val branchKeyCls = classServer.loadClass(context.branchKeys.thisType.getClassName)
      def getBranchKey(osn: Long): BranchKey = {
        val sn = subplan.getOperators.toSet.find(_.getOriginalSerialNumber == osn).get.getSerialNumber
        branchKeyCls.getField(context.branchKeys.getField(sn)).get(null).asInstanceOf[BranchKey]
      }

      assert(driver.branchKeys ===
        Set(resultMarker)
        .map(marker => getBranchKey(marker.getOriginalSerialNumber)))

      assert(driver.partitioners(getBranchKey(resultMarker.getOriginalSerialNumber)).get.numPartitions === 1)

      val result = Await.result(
        results(getBranchKey(resultMarker.getOriginalSerialNumber))
          .map { rdd =>
            assert(rdd.partitions.size === 1)
            rdd.map {
              case (_, foo: Foo) => (foo.i.get, foo.sum.get)
            }.collect.toSeq.sortBy(_._1)
          }, Duration.Inf)

      assert(result.size === 2)
      assert(result(0)._1 === 0)
      assert(result(0)._2 === (0 until 10 by 2).sum + 4 * 10)
      assert(result(1)._1 === 1)
      assert(result(1)._2 === (1 until 10 by 2).sum + 4 * 10)
    }
  }
}

object AggregateDriverClassBuilderSpec {

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

  class SortOrdering extends Ordering[ShuffleKey] {

    override def compare(x: ShuffleKey, y: ShuffleKey): Int = {
      IntOption.compareBytes(x.grouping, 0, x.grouping.length, y.grouping, 0, y.grouping.length)
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
