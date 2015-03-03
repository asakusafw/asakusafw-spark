package com.asakusafw.spark.compiler
package subplan

import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner

import java.nio.file.Files
import java.util.{ List => JList }

import scala.collection.JavaConversions._

import org.apache.spark._

import com.asakusafw.lang.compiler.api.CompilerOptions
import com.asakusafw.lang.compiler.api.mock.MockJobflowProcessorContext
import com.asakusafw.lang.compiler.model.PropertyName
import com.asakusafw.lang.compiler.model.description._
import com.asakusafw.lang.compiler.model.graph.{ Group, MarkerOperator, UserOperator }
import com.asakusafw.lang.compiler.planning.{ PlanBuilder, PlanMarker }
import com.asakusafw.runtime.core.Result
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.runtime.value._
import com.asakusafw.spark.compiler.spi.SubPlanCompiler
import com.asakusafw.spark.compiler.subplan.SubPlanType.CoGroupSubPlan
import com.asakusafw.spark.runtime.driver._
import com.asakusafw.spark.runtime.orderings._
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

    val opcls = classOf[CoGroupOperator]
    val method = opcls.getMethod(
      "cogroup",
      classOf[JList[Hoge]],
      classOf[JList[Foo]],
      classOf[Result[Hoge]],
      classOf[Result[Foo]],
      classOf[Result[Hoge]],
      classOf[Result[Foo]],
      classOf[Result[Int]],
      classOf[Int])
    val annotation = method.getAnnotation(classOf[CoGroup])
    val operator = UserOperator.builder(
      AnnotationDescription.of(annotation),
      MethodDescription.of(method),
      ClassDescription.of(opcls))
      .input("hogeList", ClassDescription.of(classOf[Hoge]),
        new Group(Seq(PropertyName.of("id")), Seq.empty[Group.Ordering]),
        hogeListMarker.getOutput)
      .input("fooList", ClassDescription.of(classOf[Foo]),
        new Group(
          Seq(PropertyName.of("hogeId")),
          Seq(new Group.Ordering(PropertyName.of("id"), Group.Direction.ASCENDANT))),
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

    val subplan = PlanBuilder.from(Seq(operator))
      .add(
        Seq(hogeListMarker, fooListMarker),
        Seq(hogeResultMarker, fooResultMarker,
          hogeErrorMarker, fooErrorMarker,
          hogeAllMarker, fooAllMarker,
          nResultMarker)).build().getPlan()
    assert(subplan.getElements.size === 1)

    val compiler = resolvers(CoGroupSubPlan)
    val context = compiler.Context(
      flowId = "flowId",
      jpContext = new MockJobflowProcessorContext(
        new CompilerOptions("buildid", "", Map.empty[String, String]),
        Thread.currentThread.getContextClassLoader,
        classServer.root.toFile))
    val thisType = compiler.compile(subplan.getElements.head)(context)
    val cls = classServer.loadClass(thisType).asSubclass(classOf[SubPlanDriver[Long]])
    assert(cls.getField("branchKeys").get(null).asInstanceOf[Set[Long]] ===
      Set(hogeResultMarker, fooResultMarker,
        hogeErrorMarker, fooErrorMarker,
        hogeAllMarker, fooAllMarker,
        nResultMarker).map(_.getOriginalSerialNumber))

    val hogeOrd = implicitly[Ordering[(IntOption, Boolean)]]
    val hogeList = sc.parallelize(0 until 10).map { i =>
      val hoge = new Hoge()
      hoge.id.modify(i)
      ((hoge.id, hoge.id.get % 3 == 0), hoge)
    }
    val fooOrd = implicitly[Ordering[(IntOption, Int)]]
    val fooList = sc.parallelize(0 until 10).flatMap(i => (0 until i).map { j =>
      val foo = new Foo()
      foo.id.modify(10 + j)
      foo.hogeId.modify(i)
      ((foo.hogeId, foo.id.toString.hashCode), foo)
    })
    val part = new GroupingPartitioner(2)
    val groupingOrd = new GroupingOrdering
    val driver = cls.getConstructor(classOf[SparkContext], classOf[Seq[_]], classOf[Partitioner], classOf[Ordering[_]])
      .newInstance(sc, Seq((hogeList, Some(hogeOrd)), (fooList, Some(fooOrd))), part, groupingOrd)
    val results = driver.execute()

    val hogeResult = results(hogeResultMarker.getOriginalSerialNumber).collect.toSeq.map(_._2.asInstanceOf[Hoge])
    assert(hogeResult.size === 1)
    assert(hogeResult(0).id.get === 1)

    val fooResult = results(fooResultMarker.getOriginalSerialNumber).collect.toSeq.map(_._2.asInstanceOf[Foo])
    assert(fooResult.size === 1)
    assert(fooResult(0).id.get === 10)
    assert(fooResult(0).hogeId.get === 1)

    val hogeError = results(hogeErrorMarker.getOriginalSerialNumber).collect.toSeq.map(_._2.asInstanceOf[Hoge]).sortBy(_.id)
    assert(hogeError.size === 9)
    assert(hogeError(0).id.get === 0)
    for (i <- 2 until 10) {
      assert(hogeError(i - 1).id.get === i)
    }

    val fooError = results(fooErrorMarker.getOriginalSerialNumber).collect.toSeq.map(_._2.asInstanceOf[Foo]).sortBy(_.hogeId)
    assert(fooError.size === 44)
    for {
      i <- 2 until 10
      j <- 0 until i
    } {
      assert(fooError((i * (i - 1)) / 2 + j - 1).id.get == 10 + j)
      assert(fooError((i * (i - 1)) / 2 + j - 1).hogeId.get == i)
    }

    val hogeAll = results(hogeAllMarker.getOriginalSerialNumber).collect.toSeq.map(_._2.asInstanceOf[Hoge]).sortBy(_.id)
    assert(hogeAll.size === 10)
    for (i <- 0 until 10) {
      assert(hogeAll(i).id.get === i)
    }

    val fooAll = results(fooAllMarker.getOriginalSerialNumber).collect.toSeq.map(_._2.asInstanceOf[Foo]).sortBy(_.hogeId)
    assert(fooAll.size === 45)
    for {
      i <- 0 until 10
      j <- 0 until i
    } {
      assert(fooAll((i * (i - 1)) / 2 + j).id.get == 10 + j)
      assert(fooAll((i * (i - 1)) / 2 + j).hogeId.get == i)
    }

    val nResult = results(nResultMarker.getOriginalSerialNumber).collect.toSeq.map(_._2.asInstanceOf[N])
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

  class GroupingPartitioner(val numPartitions: Int) extends Partitioner {

    override def getPartition(key: Any): Int = {
      val group = key.asInstanceOf[Product].productElement(0)
      val part = group.hashCode % numPartitions
      if (part < 0) part + numPartitions else part
    }
  }

  class GroupingOrdering extends Ordering[Product] {

    override def compare(x: Product, y: Product): Int = {
      implicitly[Ordering[IntOption]].compare(
        x.productElement(0).asInstanceOf[IntOption],
        y.productElement(0).asInstanceOf[IntOption])
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
  }
}
