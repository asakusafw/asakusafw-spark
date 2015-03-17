package com.asakusafw.spark.compiler.operator.aggregation

import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner

import java.nio.file.Files

import scala.collection.JavaConversions._

import com.asakusafw.lang.compiler.api.CompilerOptions
import com.asakusafw.lang.compiler.api.testing.MockJobflowProcessorContext
import com.asakusafw.lang.compiler.model.description._
import com.asakusafw.lang.compiler.model.testing.OperatorExtractor
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.runtime.value._
import com.asakusafw.spark.compiler.spi.AggregationCompiler
import com.asakusafw.spark.runtime.fragment._
import com.asakusafw.spark.tools.asm._
import com.asakusafw.vocabulary.flow.processor.PartialAggregation
import com.asakusafw.vocabulary.operator.Fold

@RunWith(classOf[JUnitRunner])
class FoldAggregationCompilerSpecTest extends FoldAggregationCompilerSpec

class FoldAggregationCompilerSpec extends FlatSpec with LoadClassSugar {

  import FoldAggregationCompilerSpec._

  behavior of classOf[FoldAggregationCompiler].getSimpleName

  def resolvers = AggregationCompiler(Thread.currentThread.getContextClassLoader)

  it should "compile Aggregation for Fold" in {
    val operator = OperatorExtractor
      .extract(classOf[Fold], classOf[FoldOperator], "fold")
      .input("input", ClassDescription.of(classOf[Hoge]))
      .output("output", ClassDescription.of(classOf[Hoge]))
      .argument("n", ImmediateDescription.of(10))
      .build()

    val compiler = resolvers(classOf[Fold])
    val classpath = Files.createTempDirectory("FoldAggregationCompilerSpec").toFile
    val context = compiler.Context(
      flowId = "flowId",
      jpContext = new MockJobflowProcessorContext(
        new CompilerOptions("buildid", "", Map.empty[String, String]),
        Thread.currentThread.getContextClassLoader,
        classpath))
    val thisType = compiler.compile(operator)(context)
    val cls = loadClass(thisType.getClassName, classpath).asSubclass(classOf[Aggregation[Seq[_], Hoge, Hoge]])

    val aggregation = cls.newInstance()
    assert(aggregation.mapSideCombine === true)

    val valueCombiner = aggregation.valueCombiner()
    valueCombiner.insertAll((0 until 100).map { i =>
      val hoge = new Hoge()
      hoge.i.modify(i)
      (Seq(i % 2), hoge)
    }.iterator)
    assert(valueCombiner.toSeq.map { case (k, v) => k -> v.i.get }
      === Seq((Seq(0), (0 until 100 by 2).sum + 10 * 49), (Seq(1), (1 until 100 by 2).sum + 10 * 49)))

    val combinerCombiner = aggregation.combinerCombiner()
    combinerCombiner.insertAll((0 until 100).map { i =>
      val hoge = new Hoge()
      hoge.i.modify(i)
      (Seq(i % 2), hoge)
    }.iterator)
    assert(combinerCombiner.toSeq.map { case (k, v) => k -> v.i.get }
      === Seq((Seq(0), (0 until 100 by 2).sum + 10 * 49), (Seq(1), (1 until 100 by 2).sum + 10 * 49)))
  }
}

object FoldAggregationCompilerSpec {

  class Hoge extends DataModel[Hoge] {

    val i = new IntOption()

    override def reset(): Unit = {
      i.setNull()
    }

    override def copyFrom(other: Hoge): Unit = {
      i.copyFrom(other.i)
    }

    def getIOption: IntOption = i
  }

  class FoldOperator {

    @Fold(partialAggregation = PartialAggregation.PARTIAL)
    def fold(acc: Hoge, value: Hoge, n: Int): Unit = {
      acc.i.add(value.i)
      acc.i.add(n)
    }
  }
}
