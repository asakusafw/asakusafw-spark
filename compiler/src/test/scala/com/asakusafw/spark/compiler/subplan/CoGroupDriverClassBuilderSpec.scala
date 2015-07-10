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
import java.nio.file.Files
import java.util.{ List => JList }

import scala.collection.JavaConversions._
import scala.concurrent.{ Await, Future }
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Writable
import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import com.asakusafw.lang.compiler.model.PropertyName
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
import com.asakusafw.spark.runtime.io.WritableSerDe
import com.asakusafw.spark.runtime.orderings._
import com.asakusafw.spark.runtime.rdd.BranchKey
import com.asakusafw.spark.tools.asm._
import com.asakusafw.vocabulary.flow.processor.InputBuffer
import com.asakusafw.vocabulary.operator.CoGroup

@RunWith(classOf[JUnitRunner])
class CoGroupDriverClassBuilderSpecTest extends CoGroupDriverClassBuilderSpec

class CoGroupDriverClassBuilderSpec extends FlatSpec with SparkWithClassServerSugar with CompilerContext {

  import CoGroupDriverClassBuilderSpec._

  behavior of classOf[CoGroupDriverClassBuilder].getSimpleName

  for {
    method <- Seq("cogroup", "cogroupEscape")
    (outputType, partitioners) <- Seq(
      (SubPlanOutputInfo.OutputType.DONT_CARE, 7),
      (SubPlanOutputInfo.OutputType.PREPARE_EXTERNAL_OUTPUT, 0))
  } {
    it should s"build cogroup driver class ${method} with OutputType.${outputType}" in {
      val hogeListMarker = MarkerOperator.builder(ClassDescription.of(classOf[Hoge]))
        .attribute(classOf[PlanMarker], PlanMarker.GATHER).build()
      val fooListMarker = MarkerOperator.builder(ClassDescription.of(classOf[Foo]))
        .attribute(classOf[PlanMarker], PlanMarker.GATHER).build()

      val operator = OperatorExtractor
        .extract(classOf[CoGroup], classOf[CoGroupOperator], method)
        .input("hogeList", ClassDescription.of(classOf[Hoge]),
          Groups.parse(Seq("id")),
          hogeListMarker.getOutput)
        .input("fooList", ClassDescription.of(classOf[Foo]),
          Groups.parse(Seq("hogeId"), Seq("+id")),
          fooListMarker.getOutput)
        .output("hogeResult", ClassDescription.of(classOf[Hoge]))
        .output("fooResult", ClassDescription.of(classOf[Foo]))
        .output("hogeError", ClassDescription.of(classOf[Hoge]))
        .output("fooError", ClassDescription.of(classOf[Foo]))
        .output("nResult", ClassDescription.of(classOf[N]))
        .argument("n", ImmediateDescription.of(10))
        .build()

      val hogeResultMarker = MarkerOperator.builder(ClassDescription.of(classOf[Hoge]))
        .attribute(classOf[PlanMarker], PlanMarker.CHECKPOINT).build()
      operator.findOutput("hogeResult").connect(hogeResultMarker.getInput)

      val fooResultMarker = MarkerOperator.builder(ClassDescription.of(classOf[Foo]))
        .attribute(classOf[PlanMarker], PlanMarker.CHECKPOINT).build()
      operator.findOutput("fooResult").connect(fooResultMarker.getInput)

      val hogeErrorMarker = MarkerOperator.builder(ClassDescription.of(classOf[Hoge]))
        .attribute(classOf[PlanMarker], PlanMarker.CHECKPOINT).build()
      operator.findOutput("hogeError").connect(hogeErrorMarker.getInput)

      val fooErrorMarker = MarkerOperator.builder(ClassDescription.of(classOf[Foo]))
        .attribute(classOf[PlanMarker], PlanMarker.CHECKPOINT).build()
      operator.findOutput("fooError").connect(fooErrorMarker.getInput)

      val hogeAllMarker = MarkerOperator.builder(ClassDescription.of(classOf[Hoge]))
        .attribute(classOf[PlanMarker], PlanMarker.CHECKPOINT).build()
      operator.findOutput("hogeResult").connect(hogeAllMarker.getInput)
      operator.findOutput("hogeError").connect(hogeAllMarker.getInput)

      val fooAllMarker = MarkerOperator.builder(ClassDescription.of(classOf[Foo]))
        .attribute(classOf[PlanMarker], PlanMarker.CHECKPOINT).build()
      operator.findOutput("fooResult").connect(fooAllMarker.getInput)
      operator.findOutput("fooError").connect(fooAllMarker.getInput)

      val nResultMarker = MarkerOperator.builder(ClassDescription.of(classOf[N]))
        .attribute(classOf[PlanMarker], PlanMarker.CHECKPOINT).build()
      operator.findOutput("nResult").connect(nResultMarker.getInput)

      val plan = PlanBuilder.from(Seq(operator))
        .add(
          Seq(hogeListMarker, fooListMarker),
          Seq(hogeResultMarker, fooResultMarker,
            hogeErrorMarker, fooErrorMarker,
            hogeAllMarker, fooAllMarker,
            nResultMarker)).build().getPlan()
      assert(plan.getElements.size === 1)
      val subplan = plan.getElements.head
      subplan.putAttribute(classOf[SubPlanInfo],
        new SubPlanInfo(subplan, SubPlanInfo.DriverType.COGROUP, Seq.empty[SubPlanInfo.DriverOption], operator))

      val hogeResultOutput = subplan.getOutputs.find(_.getOperator.getOriginalSerialNumber == hogeResultMarker.getOriginalSerialNumber).get
      hogeResultOutput.putAttribute(classOf[SubPlanOutputInfo],
        new SubPlanOutputInfo(hogeResultOutput, outputType, Seq.empty[SubPlanOutputInfo.OutputOption], null, null))

      val fooResultOutput = subplan.getOutputs.find(_.getOperator.getOriginalSerialNumber == fooResultMarker.getOriginalSerialNumber).get
      fooResultOutput.putAttribute(classOf[SubPlanOutputInfo],
        new SubPlanOutputInfo(fooResultOutput, outputType, Seq.empty[SubPlanOutputInfo.OutputOption], null, null))

      val hogeErrorOutput = subplan.getOutputs.find(_.getOperator.getOriginalSerialNumber == hogeErrorMarker.getOriginalSerialNumber).get
      hogeErrorOutput.putAttribute(classOf[SubPlanOutputInfo],
        new SubPlanOutputInfo(hogeErrorOutput, outputType, Seq.empty[SubPlanOutputInfo.OutputOption], null, null))

      val fooErrorOutput = subplan.getOutputs.find(_.getOperator.getOriginalSerialNumber == fooErrorMarker.getOriginalSerialNumber).get
      fooErrorOutput.putAttribute(classOf[SubPlanOutputInfo],
        new SubPlanOutputInfo(fooErrorOutput, outputType, Seq.empty[SubPlanOutputInfo.OutputOption], null, null))

      val hogeAllOutput = subplan.getOutputs.find(_.getOperator.getOriginalSerialNumber == hogeAllMarker.getOriginalSerialNumber).get
      hogeAllOutput.putAttribute(classOf[SubPlanOutputInfo],
        new SubPlanOutputInfo(hogeAllOutput, outputType, Seq.empty[SubPlanOutputInfo.OutputOption], null, null))

      val fooAllOutput = subplan.getOutputs.find(_.getOperator.getOriginalSerialNumber == fooAllMarker.getOriginalSerialNumber).get
      fooAllOutput.putAttribute(classOf[SubPlanOutputInfo],
        new SubPlanOutputInfo(fooAllOutput, outputType, Seq.empty[SubPlanOutputInfo.OutputOption], null, null))

      val nResultOutput = subplan.getOutputs.find(_.getOperator.getOriginalSerialNumber == nResultMarker.getOriginalSerialNumber).get
      nResultOutput.putAttribute(classOf[SubPlanOutputInfo],
        new SubPlanOutputInfo(nResultOutput, outputType, Seq.empty[SubPlanOutputInfo.OutputOption], null, null))

      implicit val context = newContext("flowId", classServer.root.toFile)

      val compiler = SubPlanCompiler(subplan.getAttribute(classOf[SubPlanInfo]).getDriverType)
      val thisType = compiler.compile(subplan)
      context.jpContext.addClass(context.branchKeys)
      context.jpContext.addClass(context.broadcastIds)
      val cls = classServer.loadClass(thisType).asSubclass(classOf[CoGroupDriver])

      val hogeOrd = new HogeSortOrdering()
      val hogeList = sc.parallelize(0 until 10).map { i =>
        val hoge = new Hoge()
        hoge.id.modify(i)
        val serde = new WritableSerDe()
        (new ShuffleKey(serde.serialize(hoge.id), serde.serialize(new BooleanOption().modify(hoge.id.get % 3 == 0))), hoge)
      }
      val fooOrd = new FooSortOrdering()
      val fooList = sc.parallelize(0 until 10).flatMap(i => (0 until i).map { j =>
        val foo = new Foo()
        foo.id.modify(10 + j)
        foo.hogeId.modify(i)
        val serde = new WritableSerDe()
        (new ShuffleKey(serde.serialize(foo.hogeId), serde.serialize(new IntOption().modify(foo.id.toString.hashCode))), foo)
      })
      val grouping = new GroupingOrdering()
      val part = new HashPartitioner(2)
      val driver = cls.getConstructor(
        classOf[SparkContext],
        classOf[Broadcast[Configuration]],
        classOf[Map[BroadcastId, Broadcast[_]]],
        classOf[Seq[(Seq[Future[RDD[(ShuffleKey, _)]]], Option[Ordering[ShuffleKey]])]],
        classOf[Ordering[ShuffleKey]],
        classOf[Partitioner])
        .newInstance(
          sc,
          hadoopConf,
          Map.empty,
          Seq((Seq(Future.successful(hogeList)), Option(hogeOrd)), (Seq(Future.successful(fooList)), Option(fooOrd))),
          grouping,
          part)
      val results = driver.execute()

      assert(driver.partitioners.size === partitioners)

      val branchKeyCls = classServer.loadClass(context.branchKeys.thisType.getClassName)
      def getBranchKey(osn: Long): BranchKey = {
        val sn = subplan.getOperators.toSet.find(_.getOriginalSerialNumber == osn).get.getSerialNumber
        branchKeyCls.getField(context.branchKeys.getField(sn)).get(null).asInstanceOf[BranchKey]
      }

      assert(driver.branchKeys ===
        Set(hogeResultMarker, fooResultMarker,
          hogeErrorMarker, fooErrorMarker,
          hogeAllMarker, fooAllMarker,
          nResultMarker).map(marker => getBranchKey(marker.getOriginalSerialNumber)))

      val hogeResult = Await.result(
        results(getBranchKey(hogeResultMarker.getOriginalSerialNumber)).map {
          _.map(_._2.asInstanceOf[Hoge]).map(_.id.get)
        }, Duration.Inf)
        .collect.toSeq
      assert(hogeResult.size === 1)
      assert(hogeResult(0) === 1)

      val fooResult = Await.result(
        results(getBranchKey(fooResultMarker.getOriginalSerialNumber)).map {
          _.map(_._2.asInstanceOf[Foo]).map(foo => (foo.id.get, foo.hogeId.get))
        }, Duration.Inf)
        .collect.toSeq
      assert(fooResult.size === 1)
      assert(fooResult(0)._1 === 10)
      assert(fooResult(0)._2 === 1)

      val hogeError = Await.result(
        results(getBranchKey(hogeErrorMarker.getOriginalSerialNumber)).map {
          _.map(_._2.asInstanceOf[Hoge]).map(_.id.get)
        }, Duration.Inf)
        .collect.toSeq.sorted
      assert(hogeError.size === 9)
      assert(hogeError(0) === 0)
      for (i <- 2 until 10) {
        assert(hogeError(i - 1) === i)
      }

      val fooError = Await.result(
        results(getBranchKey(fooErrorMarker.getOriginalSerialNumber)).map {
          _.map(_._2.asInstanceOf[Foo]).map(foo => (foo.id.get, foo.hogeId.get))
        }, Duration.Inf)
        .collect.toSeq.sortBy(_._2)
      assert(fooError.size === 44)
      for {
        i <- 2 until 10
        j <- 0 until i
      } {
        assert(fooError((i * (i - 1)) / 2 + j - 1)._1 == 10 + j)
        assert(fooError((i * (i - 1)) / 2 + j - 1)._2 == i)
      }

      val hogeAll = Await.result(
        results(getBranchKey(hogeAllMarker.getOriginalSerialNumber)).map {
          _.map(_._2.asInstanceOf[Hoge]).map(_.id.get)
        }, Duration.Inf)
        .collect.toSeq.sorted
      assert(hogeAll.size === 10)
      for (i <- 0 until 10) {
        assert(hogeAll(i) === i)
      }

      val fooAll = Await.result(
        results(getBranchKey(fooAllMarker.getOriginalSerialNumber)).map {
          _.map(_._2.asInstanceOf[Foo]).map(foo => (foo.id.get, foo.hogeId.get))
        }, Duration.Inf)
        .collect.toSeq.sortBy(_._2)
      assert(fooAll.size === 45)
      for {
        i <- 0 until 10
        j <- 0 until i
      } {
        assert(fooAll((i * (i - 1)) / 2 + j)._1 == 10 + j)
        assert(fooAll((i * (i - 1)) / 2 + j)._2 == i)
      }

      val nResult = Await.result(
        results(getBranchKey(nResultMarker.getOriginalSerialNumber)).map {
          _.map(_._2.asInstanceOf[N]).map(_.n.get)
        }, Duration.Inf)
        .collect.toSeq
      assert(nResult.size === 10)
      nResult.foreach(n => assert(n === 10))
    }
  }
}

object CoGroupDriverClassBuilderSpec {

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

  class GroupingOrdering extends Ordering[ShuffleKey] {

    override def compare(x: ShuffleKey, y: ShuffleKey): Int = {
      IntOption.compareBytes(x.grouping, 0, x.grouping.length, y.grouping, 0, y.grouping.length)
    }
  }

  class HogeSortOrdering extends GroupingOrdering {

    override def compare(x: ShuffleKey, y: ShuffleKey): Int = {
      val cmp = super.compare(x, y)
      if (cmp == 0) {
        BooleanOption.compareBytes(x.ordering, 0, x.ordering.length, y.ordering, 0, y.ordering.length)
      } else {
        cmp
      }
    }
  }

  class FooSortOrdering extends GroupingOrdering {

    override def compare(x: ShuffleKey, y: ShuffleKey): Int = {
      val cmp = super.compare(x, y)
      if (cmp == 0) {
        IntOption.compareBytes(x.ordering, 0, x.ordering.length, y.ordering, 0, y.ordering.length)
      } else {
        cmp
      }
    }
  }

  class CoGroupOperator {

    private[this] val n = new N

    @CoGroup
    def cogroup(
      hogeList: JList[Hoge], fooList: JList[Foo],
      hogeResult: Result[Hoge], fooResult: Result[Foo],
      hogeError: Result[Hoge], fooError: Result[Foo],
      nResult: Result[N], n: Int): Unit = {
      if (hogeList.size == 1 && fooList.size == 1) {
        hogeResult.add(hogeList(0))
        fooResult.add(fooList(0))
      } else {
        hogeList.foreach(hogeError.add)
        fooList.foreach(fooError.add)
      }
      this.n.n.modify(n)
      nResult.add(this.n)
    }

    @CoGroup(inputBuffer = InputBuffer.ESCAPE)
    def cogroupEscape(
      hogeList: JList[Hoge], fooList: JList[Foo],
      hogeResult: Result[Hoge], fooResult: Result[Foo],
      hogeError: Result[Hoge], fooError: Result[Foo],
      nResult: Result[N], n: Int): Unit = {
      if (hogeList.size == 1 && fooList.size == 1) {
        hogeResult.add(hogeList(0))
        fooResult.add(fooList(0))
      } else {
        hogeList.foreach(hogeError.add)
        fooList.foreach(fooError.add)
      }
      this.n.n.modify(n)
      nResult.add(this.n)
    }
  }
}
