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
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import com.asakusafw.lang.compiler.model.description.{ ClassDescription, ImmediateDescription }
import com.asakusafw.lang.compiler.model.graph.{ Groups, MarkerOperator }
import com.asakusafw.lang.compiler.model.testing.OperatorExtractor
import com.asakusafw.lang.compiler.planning.{ PlanBuilder, PlanMarker }
import com.asakusafw.runtime.core.Result
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.runtime.value.IntOption
import com.asakusafw.spark.compiler.planning.{ SubPlanInfo, SubPlanOutputInfo }
import com.asakusafw.spark.compiler.spi.SubPlanCompiler
import com.asakusafw.spark.runtime.HadoopConfForEach
import com.asakusafw.spark.runtime.driver.{ BroadcastId, ExtractDriver, ShuffleKey }
import com.asakusafw.spark.runtime.rdd.BranchKey
import com.asakusafw.vocabulary.operator.Extract

@RunWith(classOf[JUnitRunner])
class ExtractDriverClassBuilderSpecTest extends ExtractDriverClassBuilderSpec

class ExtractDriverClassBuilderSpec
  extends FlatSpec
  with SparkWithClassServerForAll
  with HadoopConfForEach
  with UsingCompilerContext {

  import ExtractDriverClassBuilderSpec._

  behavior of classOf[ExtractDriverClassBuilder].getSimpleName

  for {
    (outputType, partitioners) <- Seq(
      (SubPlanOutputInfo.OutputType.DONT_CARE, 3),
      (SubPlanOutputInfo.OutputType.PREPARE_EXTERNAL_OUTPUT, 1))
  } {
    it should s"build extract driver class with OutputType.${outputType}" in {
      val foosMarker = MarkerOperator.builder(ClassDescription.of(classOf[Foo]))
        .attribute(classOf[PlanMarker], PlanMarker.CHECKPOINT).build()

      val operator = OperatorExtractor
        .extract(classOf[Extract], classOf[ExtractOperator], "extract")
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

      implicit val context = newSubPlanCompilerContext(flowId, classServer.root.toFile)

      val compiler = SubPlanCompiler(subplan.getAttribute(classOf[SubPlanInfo]).getDriverType)
      val thisType = compiler.compile(subplan)
      context.addClass(context.branchKeys)
      context.addClass(context.broadcastIds)
      val cls = classServer.loadClass(thisType).asSubclass(classOf[ExtractDriver[_]])

      val foos = sc.parallelize(0 until 10).map { i =>
        val foo = new Foo()
        foo.id.modify(i)
        ((), foo)
      }
      val driver = cls.getConstructor(
        classOf[SparkContext],
        classOf[Broadcast[Configuration]],
        classOf[Seq[Future[RDD[_]]]],
        classOf[Map[BroadcastId, Broadcast[_]]])
        .newInstance(
          sc,
          hadoopConf,
          Seq(Future.successful(foos)),
          Map.empty)
      val results = driver.execute()

      assert(driver.partitioners.size === partitioners)

      val branchKeyCls = classServer.loadClass(context.branchKeys.thisType.getClassName)
      def getBranchKey(marker: MarkerOperator): BranchKey = {
        val sn = subplan.getOperators.toSet
          .find(_.getOriginalSerialNumber == marker.getOriginalSerialNumber).get.getSerialNumber
        branchKeyCls.getField(context.branchKeys.getField(sn)).get(null).asInstanceOf[BranchKey]
      }

      assert(driver.branchKeys ===
        Set(fooResultMarker, barResultMarker,
          nResultMarker).map(getBranchKey))

      val ((fooResult, barResult), nResult) =
        Await.result(
          results(getBranchKey(fooResultMarker)).map {
            _.map {
              case (_, foo: Foo) => foo.id.get
            }.collect.toSeq
          }.zip {
            results(getBranchKey(barResultMarker)).map {
              _.map {
                case (_, bar: Bar) => bar
              }.mapPartitionsWithIndex({
                case (part, iter) => iter.map(bar => (part, bar.fooId.get, bar.id.get))
              }).collect.toSeq
            }
          }.zip {
            results(getBranchKey(nResultMarker)).map {
              _.map {
                case (_, n: N) => n.n.get
              }.collect.toSeq
            }
          }, Duration.Inf)

      assert(fooResult.size === 10)
      assert(fooResult === (0 until 10))

      assert(barResult.size === 45)
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

  for {
    (outputType, partitioners) <- Seq(
      (SubPlanOutputInfo.OutputType.DONT_CARE, 1),
      (SubPlanOutputInfo.OutputType.PREPARE_EXTERNAL_OUTPUT, 0))
  } {
    it should s"build extract driver class missing port connection with OutputType.${outputType}" in {
      val foosMarker = MarkerOperator.builder(ClassDescription.of(classOf[Foo]))
        .attribute(classOf[PlanMarker], PlanMarker.CHECKPOINT).build()

      val operator = OperatorExtractor
        .extract(classOf[Extract], classOf[ExtractOperator], "extract")
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

      subplan.findOut(fooResultMarker)
        .putAttr(
          new SubPlanOutputInfo(_,
            outputType,
            Seq.empty[SubPlanOutputInfo.OutputOption], null, null))

      implicit val context = newSubPlanCompilerContext(flowId, classServer.root.toFile)

      val compiler = SubPlanCompiler(subplan.getAttribute(classOf[SubPlanInfo]).getDriverType)
      val thisType = compiler.compile(subplan)
      context.addClass(context.branchKeys)
      context.addClass(context.broadcastIds)
      val cls = classServer.loadClass(thisType).asSubclass(classOf[ExtractDriver[_]])

      val foos = sc.parallelize(0 until 10).map { i =>
        val foo = new Foo()
        foo.id.modify(i)
        ((), foo)
      }
      val driver = cls.getConstructor(
        classOf[SparkContext],
        classOf[Broadcast[Configuration]],
        classOf[Seq[RDD[_]]],
        classOf[Map[BroadcastId, Broadcast[_]]])
        .newInstance(
          sc,
          hadoopConf,
          Seq(Future.successful(foos)),
          Map.empty)

      assert(driver.partitioners.size === partitioners)

      val results = driver.execute()

      val branchKeyCls = classServer.loadClass(context.branchKeys.thisType.getClassName)
      def getBranchKey(marker: MarkerOperator): BranchKey = {
        val sn = subplan.getOperators.toSet
          .find(_.getOriginalSerialNumber == marker.getOriginalSerialNumber).get.getSerialNumber
        branchKeyCls.getField(context.branchKeys.getField(sn)).get(null).asInstanceOf[BranchKey]
      }

      assert(driver.branchKeys === Set(fooResultMarker).map(getBranchKey))

      val fooResult = Await.result(
        results(getBranchKey(fooResultMarker)).map {
          _.map {
            case (_, foo: Foo) => foo.id.get
          }.collect.toSeq
        }, Duration.Inf)

      assert(fooResult.size === 10)
      assert(fooResult === (0 until 10))
    }
  }
}

object ExtractDriverClassBuilderSpec {

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

    private[this] val bar = new Bar()

    private[this] val n = new N

    @Extract
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
