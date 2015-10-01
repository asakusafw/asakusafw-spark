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
package operator.aggregation

import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner

import scala.collection.JavaConversions._

import com.asakusafw.lang.compiler.api.CompilerOptions
import com.asakusafw.lang.compiler.api.testing.MockJobflowProcessorContext
import com.asakusafw.lang.compiler.model.description._
import com.asakusafw.lang.compiler.model.testing.OperatorExtractor
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.runtime.value._
import com.asakusafw.spark.compiler.spi.AggregationCompiler
import com.asakusafw.spark.runtime.aggregation.Aggregation
import com.asakusafw.spark.tools.asm._
import com.asakusafw.vocabulary.flow.processor.PartialAggregation
import com.asakusafw.vocabulary.operator.Fold

@RunWith(classOf[JUnitRunner])
class FoldAggregationCompilerSpecTest extends FoldAggregationCompilerSpec

class FoldAggregationCompilerSpec extends FlatSpec with UsingCompilerContext {

  import FoldAggregationCompilerSpec._

  behavior of classOf[FoldAggregationCompiler].getSimpleName

  it should "compile Aggregation for Fold" in {
    val operator = OperatorExtractor
      .extract(classOf[Fold], classOf[FoldOperator], "fold")
      .input("input", ClassDescription.of(classOf[Hoge]))
      .output("output", ClassDescription.of(classOf[Hoge]))
      .argument("n", ImmediateDescription.of(10))
      .build()

    implicit val context = newAggregationCompilerContext("flowId")

    val thisType = AggregationCompiler.compile(operator)
    val cls = context.loadClass[Aggregation[Seq[_], Hoge, Hoge]](thisType.getClassName)

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

  it should "compile Aggregation for Fold with projective model" in {
    val operator = OperatorExtractor
      .extract(classOf[Fold], classOf[FoldOperator], "foldp")
      .input("input", ClassDescription.of(classOf[Hoge]))
      .output("output", ClassDescription.of(classOf[Hoge]))
      .argument("n", ImmediateDescription.of(10))
      .build()

    implicit val context = newAggregationCompilerContext("flowId")

    val thisType = AggregationCompiler.compile(operator)
    val cls = context.loadClass[Aggregation[Seq[_], Hoge, Hoge]](thisType.getClassName)

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

  trait HogeP {
    def getIOption: IntOption
  }

  class Hoge extends DataModel[Hoge] with HogeP {

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

    @Fold(partialAggregation = PartialAggregation.PARTIAL)
    def foldp[H <: HogeP](acc: H, value: H, n: Int): Unit = {
      acc.getIOption.add(value.getIOption)
      acc.getIOption.add(n)
    }
  }
}
