package com.asakusafw.spark.compiler
package subplan

import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner

import java.nio.file.Files
import java.util.{ List => JList }

import scala.collection.mutable
import scala.collection.JavaConversions._

import org.apache.hadoop.conf.Configuration
import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import com.asakusafw.lang.compiler.api.CompilerOptions
import com.asakusafw.lang.compiler.api.testing.MockJobflowProcessorContext
import com.asakusafw.lang.compiler.model.PropertyName
import com.asakusafw.lang.compiler.model.description._
import com.asakusafw.lang.compiler.model.graph.{ Groups, MarkerOperator }
import com.asakusafw.lang.compiler.model.testing.OperatorExtractor
import com.asakusafw.lang.compiler.planning.{ PlanBuilder, PlanMarker }
import com.asakusafw.runtime.core.Result
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.runtime.value._
import com.asakusafw.spark.compiler.planning.SubPlanInfo
import com.asakusafw.spark.compiler.spi.SubPlanCompiler
import com.asakusafw.spark.runtime.driver._
import com.asakusafw.spark.runtime.orderings._
import com.asakusafw.spark.runtime.rdd.BranchKey
import com.asakusafw.spark.tools.asm._
import com.asakusafw.vocabulary.operator.CoGroup

@RunWith(classOf[JUnitRunner])
class CoGroupDriverClassBuilderSpecTest extends CoGroupDriverClassBuilderSpec

class CoGroupDriverClassBuilderSpec extends FlatSpec with SparkWithClassServerSugar {

  import CoGroupDriverClassBuilderSpec._

  behavior of classOf[CoGroupDriverClassBuilder].getSimpleName

  def resolvers = SubPlanCompiler(Thread.currentThread.getContextClassLoader)

  it should "build cogroup driver class" in {
    val hogeListMarker = MarkerOperator.builder(ClassDescription.of(classOf[Hoge]))
      .attribute(classOf[PlanMarker], PlanMarker.GATHER).build()
    val fooListMarker = MarkerOperator.builder(ClassDescription.of(classOf[Foo]))
      .attribute(classOf[PlanMarker], PlanMarker.GATHER).build()

    val operator = OperatorExtractor
      .extract(classOf[CoGroup], classOf[CoGroupOperator], "cogroup")
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

    implicit val context = SubPlanCompiler.Context(
      flowId = "flowId",
      jpContext = new MockJobflowProcessorContext(
        new CompilerOptions("buildid", "", Map.empty[String, String]),
        Thread.currentThread.getContextClassLoader,
        classServer.root.toFile),
      externalInputs = mutable.Map.empty,
      branchKeys = new BranchKeysClassBuilder("flowId"),
      broadcastIds = new BroadcastIdsClassBuilder("flowId"),
      shuffleKeyTypes = mutable.Set.empty)

    val compiler = resolvers.find(_.support(operator)).get
    val thisType = compiler.compile(subplan)
    context.jpContext.addClass(context.branchKeys)
    val cls = classServer.loadClass(thisType).asSubclass(classOf[CoGroupDriver])

    val hogeOrd = new ShuffleKey.SortOrdering(1, Array(true))
    val hogeList = sc.parallelize(0 until 10).map { i =>
      val hoge = new Hoge()
      hoge.id.modify(i)
      (new ShuffleKey(Seq(hoge.id), Seq(new BooleanOption().modify(hoge.id.get % 3 == 0))) {}, hoge)
    }
    val fooOrd = new ShuffleKey.SortOrdering(1, Array(true))
    val fooList = sc.parallelize(0 until 10).flatMap(i => (0 until i).map { j =>
      val foo = new Foo()
      foo.id.modify(10 + j)
      foo.hogeId.modify(i)
      (new ShuffleKey(Seq(foo.hogeId), Seq(new IntOption().modify(foo.id.toString.hashCode))) {}, foo)
    })
    val grouping = new ShuffleKey.GroupingOrdering(1)
    val part = new HashPartitioner(2)
    val driver = cls.getConstructor(
      classOf[SparkContext],
      classOf[Broadcast[Configuration]],
      classOf[Map[BroadcastId, Broadcast[_]]],
      classOf[Seq[(Seq[RDD[(ShuffleKey, _)]], Option[ShuffleKey.SortOrdering])]],
      classOf[ShuffleKey.GroupingOrdering],
      classOf[Partitioner])
      .newInstance(
        sc,
        hadoopConf,
        Map.empty,
        Seq((Seq(hogeList), Option(hogeOrd)), (Seq(fooList), Option(fooOrd))),
        grouping,
        part)
    val results = driver.execute()

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

    val hogeResult = results(getBranchKey(hogeResultMarker.getOriginalSerialNumber))
      .collect.toSeq.map(_._2.asInstanceOf[Hoge])
    assert(hogeResult.size === 1)
    assert(hogeResult(0).id.get === 1)

    val fooResult = results(getBranchKey(fooResultMarker.getOriginalSerialNumber))
      .collect.toSeq.map(_._2.asInstanceOf[Foo])
    assert(fooResult.size === 1)
    assert(fooResult(0).id.get === 10)
    assert(fooResult(0).hogeId.get === 1)

    val hogeError = results(getBranchKey(hogeErrorMarker.getOriginalSerialNumber))
      .collect.toSeq.map(_._2.asInstanceOf[Hoge]).sortBy(_.id)
    assert(hogeError.size === 9)
    assert(hogeError(0).id.get === 0)
    for (i <- 2 until 10) {
      assert(hogeError(i - 1).id.get === i)
    }

    val fooError = results(getBranchKey(fooErrorMarker.getOriginalSerialNumber))
      .collect.toSeq.map(_._2.asInstanceOf[Foo]).sortBy(_.hogeId)
    assert(fooError.size === 44)
    for {
      i <- 2 until 10
      j <- 0 until i
    } {
      assert(fooError((i * (i - 1)) / 2 + j - 1).id.get == 10 + j)
      assert(fooError((i * (i - 1)) / 2 + j - 1).hogeId.get == i)
    }

    val hogeAll = results(getBranchKey(hogeAllMarker.getOriginalSerialNumber))
      .collect.toSeq.map(_._2.asInstanceOf[Hoge]).sortBy(_.id)
    assert(hogeAll.size === 10)
    for (i <- 0 until 10) {
      assert(hogeAll(i).id.get === i)
    }

    val fooAll = results(getBranchKey(fooAllMarker.getOriginalSerialNumber))
      .collect.toSeq.map(_._2.asInstanceOf[Foo]).sortBy(_.hogeId)
    assert(fooAll.size === 45)
    for {
      i <- 0 until 10
      j <- 0 until i
    } {
      assert(fooAll((i * (i - 1)) / 2 + j).id.get == 10 + j)
      assert(fooAll((i * (i - 1)) / 2 + j).hogeId.get == i)
    }

    val nResult = results(getBranchKey(nResultMarker.getOriginalSerialNumber))
      .collect.toSeq.map(_._2.asInstanceOf[N])
    assert(nResult.size === 10)
    nResult.foreach(n => assert(n.n.get === 10))
  }
}

object CoGroupDriverClassBuilderSpec {

  class Hoge extends DataModel[Hoge] {

    val id = new IntOption()

    override def reset(): Unit = {
      id.setNull()
    }
    override def copyFrom(other: Hoge): Unit = {
      id.copyFrom(other.id)
    }

    def getIdOption: IntOption = id
  }

  class Foo extends DataModel[Foo] {

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

    def getIdOption: IntOption = id
    def getHogeIdOption: IntOption = hogeId
  }

  class N extends DataModel[N] {

    val n = new IntOption()

    override def reset(): Unit = {
      n.setNull()
    }
    override def copyFrom(other: N): Unit = {
      n.copyFrom(other.n)
    }

    def getNOption: IntOption = n
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
  }
}
