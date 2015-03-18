package com.asakusafw.spark.compiler
package subplan

import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner

import scala.collection.JavaConversions._

import org.apache.spark._
import org.apache.spark.rdd.RDD

import com.asakusafw.lang.compiler.api.CompilerOptions
import com.asakusafw.lang.compiler.api.testing.MockJobflowProcessorContext
import com.asakusafw.lang.compiler.model.PropertyName
import com.asakusafw.lang.compiler.model.description._
import com.asakusafw.lang.compiler.model.graph.{ Group, MarkerOperator }
import com.asakusafw.lang.compiler.model.testing.OperatorExtractor
import com.asakusafw.lang.compiler.planning.{ PlanBuilder, PlanMarker }
import com.asakusafw.lang.compiler.planning.spark.DominantOperator
import com.asakusafw.lang.compiler.planning.spark.PartitioningParameters
import com.asakusafw.runtime.core.Result
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.runtime.value._
import com.asakusafw.spark.compiler.spi.SubPlanCompiler
import com.asakusafw.spark.runtime.driver._
import com.asakusafw.vocabulary.operator.Extract

@RunWith(classOf[JUnitRunner])
class MapDriverClassBuilderSpecTest extends MapDriverClassBuilderSpec

class MapDriverClassBuilderSpec extends FlatSpec with SparkWithClassServerSugar {

  import MapDriverClassBuilderSpec._

  behavior of classOf[MapDriverClassBuilder].getSimpleName

  def resolvers = SubPlanCompiler(Thread.currentThread.getContextClassLoader)

  it should "build map driver class" in {
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
    subplan.putAttribute(classOf[DominantOperator], new DominantOperator(operator))
    subplan.getOutputs.find(_.getOperator.getOriginalSerialNumber == fooResultMarker.getOriginalSerialNumber)
      .get
      .putAttribute(classOf[PartitioningParameters],
        new PartitioningParameters(
          new Group(
            Seq(PropertyName.of("hogeId")),
            Seq(new Group.Ordering(PropertyName.of("id"), Group.Direction.DESCENDANT)))))

    val compiler = resolvers(operator)
    val context = compiler.Context(
      flowId = "flowId",
      jpContext = new MockJobflowProcessorContext(
        new CompilerOptions("buildid", "", Map.empty[String, String]),
        Thread.currentThread.getContextClassLoader,
        classServer.root.toFile))
    val thisType = compiler.compile(subplan)(context)
    val cls = classServer.loadClass(thisType).asSubclass(classOf[MapDriver[_, Long]])

    val hoges = sc.parallelize(0 until 10).map { i =>
      val hoge = new Hoge()
      hoge.id.modify(i)
      ((), hoge)
    }
    val driver = cls.getConstructor(classOf[SparkContext], classOf[RDD[_]]).newInstance(sc, hoges)
    val results = driver.execute()

    assert(driver.branchKeys ===
      Set(hogeResultMarker, fooResultMarker,
        nResultMarker).map(_.getOriginalSerialNumber))

    val hogeResult = results(hogeResultMarker.getOriginalSerialNumber).map(_._2.asInstanceOf[Hoge]).collect.toSeq
    assert(hogeResult.size === 10)
    assert(hogeResult.map(_.id.get) === (0 until 10))

    val fooResult = results(fooResultMarker.getOriginalSerialNumber)
      .mapPartitionsWithIndex({ case (part, iter) => iter.map(foo => (part, foo._2.asInstanceOf[Foo])) }).collect.toSeq
    assert(fooResult.size === 45)
    fooResult.groupBy(_._2.hogeId.get).foreach {
      case (hogeId, foos) =>
        val part = foos.head._1
        assert(foos.tail.forall(_._1 == part))
        assert(foos.map(_._2.id.get) === (0 until hogeId).map(j => (hogeId * (hogeId - 1)) / 2 + j).reverse)
    }

    val nResult = results(nResultMarker.getOriginalSerialNumber).collect.toSeq.map(_._2.asInstanceOf[N])
    assert(nResult.size === 10)
    nResult.foreach(n => assert(n.n.get === 10))
  }

  it should "build map driver class missing port connection" in {
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
    subplan.putAttribute(classOf[DominantOperator], new DominantOperator(operator))

    val compiler = resolvers(operator)
    val context = compiler.Context(
      flowId = "flowId",
      jpContext = new MockJobflowProcessorContext(
        new CompilerOptions("buildid", "", Map.empty[String, String]),
        Thread.currentThread.getContextClassLoader,
        classServer.root.toFile))
    val thisType = compiler.compile(subplan)(context)
    val cls = classServer.loadClass(thisType).asSubclass(classOf[MapDriver[_, Long]])

    val hoges = sc.parallelize(0 until 10).map { i =>
      val hoge = new Hoge()
      hoge.id.modify(i)
      ((), hoge)
    }
    val driver = cls.getConstructor(classOf[SparkContext], classOf[RDD[_]]).newInstance(sc, hoges)
    val results = driver.execute()

    assert(driver.branchKeys === Set(hogeResultMarker).map(_.getOriginalSerialNumber))

    val hogeResult = results(hogeResultMarker.getOriginalSerialNumber).map(_._2.asInstanceOf[Hoge]).collect.toSeq
    assert(hogeResult.size === 10)
    assert(hogeResult.map(_.id.get) === (0 until 10))
  }
}

object MapDriverClassBuilderSpec {

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
