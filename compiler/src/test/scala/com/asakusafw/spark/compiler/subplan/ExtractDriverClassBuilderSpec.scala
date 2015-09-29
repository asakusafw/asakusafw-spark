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
import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import com.asakusafw.lang.compiler.model.description._
import com.asakusafw.lang.compiler.model.graph.{ Groups, MarkerOperator }
import com.asakusafw.lang.compiler.model.testing.OperatorExtractor
import com.asakusafw.lang.compiler.planning.{ PlanBuilder, PlanMarker }
import com.asakusafw.runtime.core.Result
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.runtime.value._
import com.asakusafw.spark.compiler.planning.{ SubPlanInfo, SubPlanOutputInfo }
import com.asakusafw.spark.compiler.spi.SubPlanCompiler
import com.asakusafw.spark.runtime.driver._
import com.asakusafw.spark.runtime.rdd.BranchKey
import com.asakusafw.vocabulary.operator.Extract

@RunWith(classOf[JUnitRunner])
class ExtractDriverClassBuilderSpecTest extends ExtractDriverClassBuilderSpec

class ExtractDriverClassBuilderSpec extends FlatSpec with SparkWithClassServerSugar with UsingCompilerContext {

  import ExtractDriverClassBuilderSpec._

  behavior of classOf[ExtractDriverClassBuilder].getSimpleName

  for {
    (outputType, partitioners) <- Seq(
      (SubPlanOutputInfo.OutputType.DONT_CARE, 3),
      (SubPlanOutputInfo.OutputType.PREPARE_EXTERNAL_OUTPUT, 1))
  } {
    it should s"build extract driver class with OutputType.${outputType}" in {
      val hogesMarker = MarkerOperator.builder(ClassDescription.of(classOf[Hoge]))
        .attribute(classOf[PlanMarker], PlanMarker.CHECKPOINT).build()

      val operator = OperatorExtractor
        .extract(classOf[Extract], classOf[ExtractOperator], "extract")
        .input("hogeList", ClassDescription.of(classOf[Hoge]), hogesMarker.getOutput)
        .output("hogeResult", ClassDescription.of(classOf[Hoge]))
        .output("fooResult", ClassDescription.of(classOf[Foo]))
        .output("nResult", ClassDescription.of(classOf[N]))
        .argument("n", ImmediateDescription.of(10))
        .build()

      val hogeResultMarker = MarkerOperator.builder(ClassDescription.of(classOf[Hoge]))
        .attribute(classOf[PlanMarker], PlanMarker.CHECKPOINT).build()
      operator.findOutput("hogeResult").connect(hogeResultMarker.getInput)

      val fooResultMarker = MarkerOperator.builder(ClassDescription.of(classOf[Foo]))
        .attribute(classOf[PlanMarker], PlanMarker.CHECKPOINT).build()
      operator.findOutput("fooResult").connect(fooResultMarker.getInput)

      val nResultMarker = MarkerOperator.builder(ClassDescription.of(classOf[N]))
        .attribute(classOf[PlanMarker], PlanMarker.CHECKPOINT).build()
      operator.findOutput("nResult").connect(nResultMarker.getInput)

      val plan = PlanBuilder.from(Seq(operator))
        .add(
          Seq(hogesMarker),
          Seq(hogeResultMarker, fooResultMarker,
            nResultMarker)).build().getPlan()
      assert(plan.getElements.size === 1)
      val subplan = plan.getElements.head
      subplan.putAttribute(classOf[SubPlanInfo],
        new SubPlanInfo(subplan, SubPlanInfo.DriverType.EXTRACT, Seq.empty[SubPlanInfo.DriverOption], operator))

      val hogeResultOutput = subplan.getOutputs.find(_.getOperator.getOriginalSerialNumber == hogeResultMarker.getOriginalSerialNumber).get
      hogeResultOutput.putAttribute(classOf[SubPlanOutputInfo],
        new SubPlanOutputInfo(hogeResultOutput, outputType, Seq.empty[SubPlanOutputInfo.OutputOption], null, null))

      val fooResultOutput = subplan.getOutputs.find(_.getOperator.getOriginalSerialNumber == fooResultMarker.getOriginalSerialNumber).get
      fooResultOutput.putAttribute(classOf[SubPlanOutputInfo],
        new SubPlanOutputInfo(fooResultOutput, SubPlanOutputInfo.OutputType.PARTITIONED, Seq.empty[SubPlanOutputInfo.OutputOption], Groups.parse(Seq("hogeId"), Seq("-id")), null))

      val nResultOutput = subplan.getOutputs.find(_.getOperator.getOriginalSerialNumber == nResultMarker.getOriginalSerialNumber).get
      nResultOutput.putAttribute(classOf[SubPlanOutputInfo],
        new SubPlanOutputInfo(hogeResultOutput, outputType, Seq.empty[SubPlanOutputInfo.OutputOption], null, null))

      implicit val context = newContext("flowId", classServer.root.toFile)

      val compiler = SubPlanCompiler(subplan.getAttribute(classOf[SubPlanInfo]).getDriverType)
      val thisType = compiler.compile(subplan)
      context.jpContext.addClass(context.branchKeys)
      context.jpContext.addClass(context.broadcastIds)
      val cls = classServer.loadClass(thisType).asSubclass(classOf[ExtractDriver[_]])

      val hoges = sc.parallelize(0 until 10).map { i =>
        val hoge = new Hoge()
        hoge.id.modify(i)
        ((), hoge)
      }
      val driver = cls.getConstructor(
        classOf[SparkContext],
        classOf[Broadcast[Configuration]],
        classOf[Seq[Future[RDD[_]]]],
        classOf[Map[BroadcastId, Broadcast[_]]])
        .newInstance(
          sc,
          hadoopConf,
          Seq(Future.successful(hoges)),
          Map.empty)
      val results = driver.execute()

      assert(driver.partitioners.size === partitioners)

      val branchKeyCls = classServer.loadClass(context.branchKeys.thisType.getClassName)
      def getBranchKey(osn: Long): BranchKey = {
        val sn = subplan.getOperators.toSet.find(_.getOriginalSerialNumber == osn).get.getSerialNumber
        branchKeyCls.getField(context.branchKeys.getField(sn)).get(null).asInstanceOf[BranchKey]
      }

      assert(driver.branchKeys ===
        Set(hogeResultMarker, fooResultMarker,
          nResultMarker).map(marker => getBranchKey(marker.getOriginalSerialNumber)))

      val hogeResult = Await.result(
        results(getBranchKey(hogeResultMarker.getOriginalSerialNumber)).map {
          _.map(_._2.asInstanceOf[Hoge]).map(_.id.get)
        }, Duration.Inf)
        .collect.toSeq
      assert(hogeResult.size === 10)
      assert(hogeResult === (0 until 10))

      val fooResult = Await.result(
        results(getBranchKey(fooResultMarker.getOriginalSerialNumber)).map {
          _.map(_._2.asInstanceOf[Foo]).mapPartitionsWithIndex({
            case (part, iter) => iter.map(foo => (part, foo.hogeId.get, foo.id.get))
          })
        }, Duration.Inf)
        .collect.toSeq
      assert(fooResult.size === 45)
      fooResult.groupBy(_._2).foreach {
        case (hogeId, foos) =>
          val part = foos.head._1
          assert(foos.tail.forall(_._1 == part))
          assert(foos.map(_._3) === (0 until hogeId).map(j => (hogeId * (hogeId - 1)) / 2 + j).reverse)
      }

      val nResult = Await.result(
        results(getBranchKey(nResultMarker.getOriginalSerialNumber)).map {
          _.map(_._2.asInstanceOf[N]).map(_.n.get)
        }, Duration.Inf).collect.toSeq
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
      val hogesMarker = MarkerOperator.builder(ClassDescription.of(classOf[Hoge]))
        .attribute(classOf[PlanMarker], PlanMarker.CHECKPOINT).build()

      val operator = OperatorExtractor
        .extract(classOf[Extract], classOf[ExtractOperator], "extract")
        .input("hogeList", ClassDescription.of(classOf[Hoge]), hogesMarker.getOutput)
        .output("hogeResult", ClassDescription.of(classOf[Hoge]))
        .output("fooResult", ClassDescription.of(classOf[Foo]))
        .output("nResult", ClassDescription.of(classOf[N]))
        .argument("n", ImmediateDescription.of(10))
        .build()

      val hogeResultMarker = MarkerOperator.builder(ClassDescription.of(classOf[Hoge]))
        .attribute(classOf[PlanMarker], PlanMarker.CHECKPOINT).build()
      operator.findOutput("hogeResult").connect(hogeResultMarker.getInput)

      val plan = PlanBuilder.from(Seq(operator))
        .add(
          Seq(hogesMarker),
          Seq(hogeResultMarker)).build().getPlan()
      assert(plan.getElements.size === 1)
      val subplan = plan.getElements.head
      subplan.putAttribute(classOf[SubPlanInfo],
        new SubPlanInfo(subplan, SubPlanInfo.DriverType.EXTRACT, Seq.empty[SubPlanInfo.DriverOption], operator))

      val hogeResultOutput = subplan.getOutputs.find(_.getOperator.getOriginalSerialNumber == hogeResultMarker.getOriginalSerialNumber).get
      hogeResultOutput.putAttribute(classOf[SubPlanOutputInfo],
        new SubPlanOutputInfo(hogeResultOutput, outputType, Seq.empty[SubPlanOutputInfo.OutputOption], null, null))

      implicit val context = newContext("flowId", classServer.root.toFile)

      val compiler = SubPlanCompiler(subplan.getAttribute(classOf[SubPlanInfo]).getDriverType)
      val thisType = compiler.compile(subplan)
      context.jpContext.addClass(context.branchKeys)
      context.jpContext.addClass(context.broadcastIds)
      val cls = classServer.loadClass(thisType).asSubclass(classOf[ExtractDriver[_]])

      val hoges = sc.parallelize(0 until 10).map { i =>
        val hoge = new Hoge()
        hoge.id.modify(i)
        ((), hoge)
      }
      val driver = cls.getConstructor(
        classOf[SparkContext],
        classOf[Broadcast[Configuration]],
        classOf[Seq[RDD[_]]],
        classOf[Map[BroadcastId, Broadcast[_]]])
        .newInstance(
          sc,
          hadoopConf,
          Seq(Future.successful(hoges)),
          Map.empty)

      assert(driver.partitioners.size === partitioners)

      val results = driver.execute()

      val branchKeyCls = classServer.loadClass(context.branchKeys.thisType.getClassName)
      def getBranchKey(osn: Long): BranchKey = {
        val sn = subplan.getOperators.toSet.find(_.getOriginalSerialNumber == osn).get.getSerialNumber
        branchKeyCls.getField(context.branchKeys.getField(sn)).get(null).asInstanceOf[BranchKey]
      }

      assert(driver.branchKeys ===
        Set(hogeResultMarker).map(marker => getBranchKey(marker.getOriginalSerialNumber)))

      val hogeResult = Await.result(
        results(getBranchKey(hogeResultMarker.getOriginalSerialNumber)).map {
          _.map(_._2.asInstanceOf[Hoge]).map(_.id.get)
        }, Duration.Inf)
        .collect.toSeq
      assert(hogeResult.size === 10)
      assert(hogeResult === (0 until 10))
    }
  }
}

object ExtractDriverClassBuilderSpec {

  class Hoge extends DataModel[Hoge] with Writable {

    val id = new IntOption()

    override def reset(): Unit = {
      id.setNull()
    }
    override def copyFrom(other: Hoge): Unit = {
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

  class Foo extends DataModel[Foo] with Writable {

    val id = new IntOption()
    val hogeId = new IntOption()

    override def reset(): Unit = {
      id.setNull()
      hogeId.setNull()
    }
    override def copyFrom(other: Foo): Unit = {
      id.copyFrom(other.id)
      hogeId.copyFrom(other.hogeId)
    }
    override def readFields(in: DataInput): Unit = {
      id.readFields(in)
      hogeId.readFields(in)
    }
    override def write(out: DataOutput): Unit = {
      id.write(out)
      hogeId.write(out)
    }

    def getIdOption: IntOption = id
    def getHogeIdOption: IntOption = hogeId
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

    private[this] val foo = new Foo()

    private[this] val n = new N

    @Extract
    def extract(
      hoge: Hoge,
      hogeResult: Result[Hoge], fooResult: Result[Foo],
      nResult: Result[N], n: Int): Unit = {
      hogeResult.add(hoge)
      for (i <- 0 until hoge.id.get) {
        foo.reset()
        foo.id.modify((hoge.id.get * (hoge.id.get - 1)) / 2 + i)
        foo.hogeId.copyFrom(hoge.id)
        fooResult.add(foo)
      }
      this.n.n.modify(n)
      nResult.add(this.n)
    }
  }
}
