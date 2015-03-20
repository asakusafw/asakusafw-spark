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
import com.asakusafw.spark.runtime.orderings._
import com.asakusafw.vocabulary.flow.processor.PartialAggregation
import com.asakusafw.vocabulary.operator.Fold

@RunWith(classOf[JUnitRunner])
class AggregateDriverClassBuilderSpecTest extends AggregateDriverClassBuilderSpec

class AggregateDriverClassBuilderSpec extends FlatSpec with SparkWithClassServerSugar {

  import AggregateDriverClassBuilderSpec._

  behavior of classOf[AggregateDriverClassBuilder].getSimpleName

  def resolvers = SubPlanCompiler(Thread.currentThread.getContextClassLoader)

  it should "build aggregate driver class" in {
    val hogesMarker = MarkerOperator.builder(ClassDescription.of(classOf[Hoge]))
      .attribute(classOf[PlanMarker], PlanMarker.CHECKPOINT).build()

    val operator = OperatorExtractor
      .extract(classOf[Fold], classOf[FoldOperator], "fold")
      .input("hoges", ClassDescription.of(classOf[Hoge]), hogesMarker.getOutput)
      .output("result", ClassDescription.of(classOf[Hoge]))
      .argument("n", ImmediateDescription.of(10))
      .build()

    val resultMarker = MarkerOperator.builder(ClassDescription.of(classOf[Hoge]))
      .attribute(classOf[PlanMarker], PlanMarker.CHECKPOINT).build()
    operator.findOutput("result").connect(resultMarker.getInput)

    val plan = PlanBuilder.from(Seq(operator))
      .add(
        Seq(hogesMarker),
        Seq(resultMarker)).build().getPlan()
    assert(plan.getElements.size === 1)
    val subplan = plan.getElements.head
    subplan.putAttribute(classOf[DominantOperator], new DominantOperator(operator))
    subplan.getOutputs.find(_.getOperator.getOriginalSerialNumber == resultMarker.getOriginalSerialNumber)
      .get
      .putAttribute(classOf[PartitioningParameters],
        new PartitioningParameters(
          new Group(Seq(PropertyName.of("i")), Seq.empty[Group.Ordering])))

    val compiler = resolvers(operator)
    val context = compiler.Context(
      flowId = "flowId",
      jpContext = new MockJobflowProcessorContext(
        new CompilerOptions("buildid", "", Map.empty[String, String]),
        Thread.currentThread.getContextClassLoader,
        classServer.root.toFile))
    val thisType = compiler.compile(subplan)(context)
    val cls = classServer.loadClass(thisType).asSubclass(classOf[AggregateDriver[Seq[_], Hoge, Hoge, Long]])

    val hoges = sc.parallelize(0 until 10).map { i =>
      val hoge = new Hoge()
      hoge.i.modify(i % 2)
      hoge.sum.modify(i)
      (Seq(hoge.i), hoge)
    }
    val driver = cls.getConstructor(classOf[SparkContext], classOf[Seq[RDD[_]]], classOf[Partitioner])
      .newInstance(sc, Seq(hoges), new HashPartitioner(2))
    val results = driver.execute()

    assert(driver.branchKeys === Set(resultMarker.getOriginalSerialNumber))

    val result = results(resultMarker.getOriginalSerialNumber).asInstanceOf[RDD[(Seq[IntOption], Hoge)]]
      .collect.toSeq.sortBy(_._1.head)
    assert(result.size === 2)
    assert(result(0)._1.head.get === 0)
    assert(result(0)._2.getSumOption.get === (0 until 10 by 2).sum + 4 * 10)
    assert(result(1)._1.head.get === 1)
    assert(result(1)._2.getSumOption.get === (1 until 10 by 2).sum + 4 * 10)
  }
}

object AggregateDriverClassBuilderSpec {

  class Hoge extends DataModel[Hoge] {

    val i = new IntOption()
    val sum = new IntOption()

    override def reset(): Unit = {
      i.setNull()
      sum.setNull()
    }
    override def copyFrom(other: Hoge): Unit = {
      i.copyFrom(other.i)
      sum.copyFrom(other.sum)
    }

    def getIOption: IntOption = i
    def getSumOption: IntOption = sum
  }

  class FoldOperator {

    @Fold(partialAggregation = PartialAggregation.PARTIAL)
    def fold(acc: Hoge, value: Hoge, n: Int): Unit = {
      acc.sum.add(value.sum)
      acc.sum.add(n)
    }
  }
}
