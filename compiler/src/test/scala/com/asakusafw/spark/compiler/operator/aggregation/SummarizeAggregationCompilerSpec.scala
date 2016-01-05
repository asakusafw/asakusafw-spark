/*
 * Copyright 2011-2016 Asakusa Framework Team.
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

import com.asakusafw.lang.compiler.model.description.ClassDescription
import com.asakusafw.lang.compiler.model.testing.OperatorExtractor
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.runtime.value._
import com.asakusafw.spark.compiler.spi.AggregationCompiler
import com.asakusafw.spark.runtime.aggregation.Aggregation
import com.asakusafw.spark.tools.asm._
import com.asakusafw.vocabulary.flow.processor.PartialAggregation
import com.asakusafw.vocabulary.model.{ Key, Summarized }
import com.asakusafw.vocabulary.operator.Summarize

@RunWith(classOf[JUnitRunner])
class SummarizeAggregationCompilerSpecTest extends SummarizeAggregationCompilerSpec

class SummarizeAggregationCompilerSpec extends FlatSpec with UsingCompilerContext {

  import SummarizeAggregationCompilerSpec._

  behavior of classOf[SummarizeAggregationCompiler].getSimpleName

  it should "compile Aggregation for Summarize" in {
    val operator = OperatorExtractor
      .extract(classOf[Summarize], classOf[SummarizeOperator], "summarize")
      .input("input", ClassDescription.of(classOf[Value]))
      .output("output", ClassDescription.of(classOf[SummarizedValue]))
      .build()

    implicit val context = newAggregationCompilerContext("flowId")

    val thisType = AggregationCompiler.compile(operator)
    val cls = context.loadClass[Aggregation[Seq[_], Value, SummarizedValue]](thisType.getClassName)

    val aggregation = cls.newInstance()
    assert(aggregation.mapSideCombine === true)

    val valueCombiner = aggregation.valueCombiner()
    valueCombiner.insertAll((0 until 100).map { i =>
      val value = new Value()
      value.key.modify(i % 2)
      value.zOpt.modify(i % 2 == 0)
      value.bOpt.modify(i.toByte)
      value.sOpt.modify(i.toShort)
      value.iOpt.modify(i)
      value.jOpt.modify(i.toLong)
      value.fOpt.modify(i.toFloat)
      value.dOpt.modify(i.toDouble)
      value.decOpt.modify(BigDecimal(i).underlying)
      value.strOpt.modify(i.toString)
      value.dateOpt.modify(i)
      value.dtOpt.modify(i)
      (Seq(i % 2), value)
    }.iterator)

    assert(
      valueCombiner.toSeq.map {
        case (_, summarized) =>
          (summarized.key.get, {
            (summarized.zAny.get,
              summarized.bAny.get,
              summarized.sAny.get,
              summarized.iAny.get,
              summarized.jAny.get,
              summarized.fAny.get,
              summarized.dAny.get,
              BigDecimal(summarized.decAny.get),
              summarized.strAny.getAsString,
              summarized.dateAny.get,
              summarized.dtAny.get)
          }, {
            (summarized.bSum.get,
              summarized.sSum.get,
              summarized.iSum.get,
              summarized.jSum.get,
              summarized.fSum.get,
              summarized.dSum.get,
              BigDecimal(summarized.decSum.get))
          }, {
            (summarized.zMax.get,
              summarized.bMax.get,
              summarized.sMax.get,
              summarized.iMax.get,
              summarized.jMax.get,
              summarized.fMax.get,
              summarized.dMax.get,
              BigDecimal(summarized.decMax.get),
              summarized.strMax.getAsString,
              summarized.dateMax.get,
              summarized.dtMax.get)
          }, {
            (summarized.zMin.get,
              summarized.bMin.get,
              summarized.sMin.get,
              summarized.iMin.get,
              summarized.jMin.get,
              summarized.fMin.get,
              summarized.dMin.get,
              BigDecimal(summarized.decMin.get),
              summarized.strMin.getAsString,
              summarized.dateMin.get,
              summarized.dtMin.get)
          },
            summarized.count.get)
      } ===
        Seq(
          (0, {
            (true,
              0.toByte,
              0.toShort,
              0,
              0.toLong,
              0.toFloat,
              0.toDouble,
              BigDecimal(0),
              0.toString,
              new Date(0),
              new DateTime(0))
          }, {
            ((0 until 100 by 2).sum.toLong,
              (0 until 100 by 2).sum.toLong,
              (0 until 100 by 2).sum.toLong,
              (0 until 100 by 2).sum.toLong,
              (0 until 100 by 2).sum.toDouble,
              (0 until 100 by 2).sum.toDouble,
              BigDecimal((0 until 100 by 2).sum))
          }, {
            (true,
              98.toByte,
              98.toShort,
              98,
              98.toLong,
              98.toFloat,
              98.toDouble,
              BigDecimal(98),
              98.toString,
              new Date(98),
              new DateTime(98))
          }, {
            (true,
              0.toByte,
              0.toShort,
              0,
              0.toLong,
              0.toFloat,
              0.toDouble,
              BigDecimal(0),
              0.toString,
              new Date(0),
              new DateTime(0))
          },
            50),
          (1, {
            (false,
              1.toByte,
              1.toShort,
              1,
              1.toLong,
              1.toFloat,
              1.toDouble,
              BigDecimal(1),
              1.toString,
              new Date(1),
              new DateTime(1))
          }, {
            ((1 until 100 by 2).sum.toLong,
              (1 until 100 by 2).sum.toLong,
              (1 until 100 by 2).sum.toLong,
              (1 until 100 by 2).sum.toLong,
              (1 until 100 by 2).sum.toDouble,
              (1 until 100 by 2).sum.toDouble,
              BigDecimal((1 until 100 by 2).sum))
          }, {
            (false,
              99.toByte,
              99.toShort,
              99,
              99.toLong,
              99.toFloat,
              99.toDouble,
              BigDecimal(99),
              99.toString,
              new Date(99),
              new DateTime(99))
          }, {
            (false,
              1.toByte,
              1.toShort,
              1,
              1.toLong,
              1.toFloat,
              1.toDouble,
              BigDecimal(1),
              1.toString,
              new Date(1),
              new DateTime(1))
          },
            50)))

    val combinerCombiner = aggregation.combinerCombiner()
    combinerCombiner.insertAll((0 until 100).map { i =>
      val value = new SummarizedValue()
      value.key.modify(i % 2)

      value.zAny.modify(i % 2 == 0)
      value.bAny.modify(i.toByte)
      value.sAny.modify(i.toShort)
      value.iAny.modify(i)
      value.jAny.modify(i.toLong)
      value.fAny.modify(i.toFloat)
      value.dAny.modify(i.toDouble)
      value.decAny.modify(BigDecimal(i).underlying)
      value.strAny.modify(i.toString)
      value.dateAny.modify(i)
      value.dtAny.modify(i)

      value.bSum.modify(i.toLong)
      value.sSum.modify(i.toLong)
      value.iSum.modify(i.toLong)
      value.jSum.modify(i.toLong)
      value.fSum.modify(i.toDouble)
      value.dSum.modify(i.toDouble)
      value.decSum.modify(BigDecimal(i).underlying)

      value.zMax.modify(i % 2 == 0)
      value.bMax.modify(i.toByte)
      value.sMax.modify(i.toShort)
      value.iMax.modify(i)
      value.jMax.modify(i.toLong)
      value.fMax.modify(i.toFloat)
      value.dMax.modify(i.toDouble)
      value.decMax.modify(BigDecimal(i).underlying)
      value.strMax.modify(i.toString)
      value.dateMax.modify(i)
      value.dtMax.modify(i)

      value.zMin.modify(i % 2 == 0)
      value.bMin.modify(i.toByte)
      value.sMin.modify(i.toShort)
      value.iMin.modify(i)
      value.jMin.modify(i.toLong)
      value.fMin.modify(i.toFloat)
      value.dMin.modify(i.toDouble)
      value.decMin.modify(BigDecimal(i).underlying)
      value.strMin.modify(i.toString)
      value.dateMin.modify(i)
      value.dtMin.modify(i)

      value.count.modify(i)

      (Seq(i % 2), value)
    }.iterator)

    assert(
      combinerCombiner.toSeq.map {
        case (_, summarized) =>
          (summarized.key.get, {
            (summarized.zAny.get,
              summarized.bAny.get,
              summarized.sAny.get,
              summarized.iAny.get,
              summarized.jAny.get,
              summarized.fAny.get,
              summarized.dAny.get,
              BigDecimal(summarized.decAny.get),
              summarized.strAny.getAsString,
              summarized.dateAny.get,
              summarized.dtAny.get)
          }, {
            (summarized.bSum.get,
              summarized.sSum.get,
              summarized.iSum.get,
              summarized.jSum.get,
              summarized.fSum.get,
              summarized.dSum.get,
              BigDecimal(summarized.decSum.get))
          }, {
            (summarized.zMax.get,
              summarized.bMax.get,
              summarized.sMax.get,
              summarized.iMax.get,
              summarized.jMax.get,
              summarized.fMax.get,
              summarized.dMax.get,
              BigDecimal(summarized.decMax.get),
              summarized.strMax.getAsString,
              summarized.dateMax.get,
              summarized.dtMax.get)
          }, {
            (summarized.zMin.get,
              summarized.bMin.get,
              summarized.sMin.get,
              summarized.iMin.get,
              summarized.jMin.get,
              summarized.fMin.get,
              summarized.dMin.get,
              BigDecimal(summarized.decMin.get),
              summarized.strMin.getAsString,
              summarized.dateMin.get,
              summarized.dtMin.get)
          },
            summarized.count.get)
      } ===
        Seq(
          (0, {
            (true,
              0.toByte,
              0.toShort,
              0,
              0.toLong,
              0.toFloat,
              0.toDouble,
              BigDecimal(0),
              0.toString,
              new Date(0),
              new DateTime(0))
          }, {
            ((0 until 100 by 2).sum.toLong,
              (0 until 100 by 2).sum.toLong,
              (0 until 100 by 2).sum.toLong,
              (0 until 100 by 2).sum.toLong,
              (0 until 100 by 2).sum.toDouble,
              (0 until 100 by 2).sum.toDouble,
              BigDecimal((0 until 100 by 2).sum))
          }, {
            (true,
              98.toByte,
              98.toShort,
              98,
              98.toLong,
              98.toFloat,
              98.toDouble,
              BigDecimal(98),
              98.toString,
              new Date(98),
              new DateTime(98))
          }, {
            (true,
              0.toByte,
              0.toShort,
              0,
              0.toLong,
              0.toFloat,
              0.toDouble,
              BigDecimal(0),
              0.toString,
              new Date(0),
              new DateTime(0))
          },
            2450),
          (1, {
            (false,
              1.toByte,
              1.toShort,
              1,
              1.toLong,
              1.toFloat,
              1.toDouble,
              BigDecimal(1),
              1.toString,
              new Date(1),
              new DateTime(1))
          }, {
            ((1 until 100 by 2).sum.toLong,
              (1 until 100 by 2).sum.toLong,
              (1 until 100 by 2).sum.toLong,
              (1 until 100 by 2).sum.toLong,
              (1 until 100 by 2).sum.toDouble,
              (1 until 100 by 2).sum.toDouble,
              BigDecimal((1 until 100 by 2).sum))
          }, {
            (false,
              99.toByte,
              99.toShort,
              99,
              99.toLong,
              99.toFloat,
              99.toDouble,
              BigDecimal(99),
              99.toString,
              new Date(99),
              new DateTime(99))
          }, {
            (false,
              1.toByte,
              1.toShort,
              1,
              1.toLong,
              1.toFloat,
              1.toDouble,
              BigDecimal(1),
              1.toString,
              new Date(1),
              new DateTime(1))
          },
            2500)))
  }
}

object SummarizeAggregationCompilerSpec {

  class Value extends DataModel[Value] {

    val key = new IntOption()
    val zOpt = new BooleanOption()
    val bOpt = new ByteOption()
    val sOpt = new ShortOption()
    val iOpt = new IntOption()
    val jOpt = new LongOption()
    val fOpt = new FloatOption()
    val dOpt = new DoubleOption()
    val decOpt = new DecimalOption()
    val strOpt = new StringOption()
    val dateOpt = new DateOption()
    val dtOpt = new DateTimeOption()

    override def reset(): Unit = {
      key.setNull()
      zOpt.setNull()
      bOpt.setNull()
      sOpt.setNull()
      iOpt.setNull()
      jOpt.setNull()
      fOpt.setNull()
      dOpt.setNull()
      decOpt.setNull()
      strOpt.setNull()
      dateOpt.setNull()
      dtOpt.setNull()
    }

    override def copyFrom(other: Value): Unit = {
      key.copyFrom(other.key)
      zOpt.copyFrom(other.zOpt)
      bOpt.copyFrom(other.bOpt)
      sOpt.copyFrom(other.sOpt)
      iOpt.copyFrom(other.iOpt)
      jOpt.copyFrom(other.jOpt)
      fOpt.copyFrom(other.fOpt)
      dOpt.copyFrom(other.dOpt)
      decOpt.copyFrom(other.decOpt)
      strOpt.copyFrom(other.strOpt)
      dateOpt.copyFrom(other.dateOpt)
      dtOpt.copyFrom(other.dtOpt)
    }

    def getKeyOption = key
    def getZOptOption = zOpt
    def getBOptOption = bOpt
    def getSOptOption = sOpt
    def getIOptOption = iOpt
    def getJOptOption = jOpt
    def getFOptOption = fOpt
    def getDOptOption = dOpt
    def getDecOptOption = decOpt
    def getStrOptOption = strOpt
    def getDateOptOption = dateOpt
    def getDtOptOption = dtOpt
  }

  @Summarized(term = new Summarized.Term(
    source = classOf[Value],
    shuffle = new Key(group = Array("key")),
    foldings = Array(
      new Summarized.Folding(source = "key", destination = "key", aggregator = Summarized.Aggregator.ANY),

      new Summarized.Folding(source = "zOpt", destination = "zAny", aggregator = Summarized.Aggregator.ANY),
      new Summarized.Folding(source = "bOpt", destination = "bAny", aggregator = Summarized.Aggregator.ANY),
      new Summarized.Folding(source = "sOpt", destination = "sAny", aggregator = Summarized.Aggregator.ANY),
      new Summarized.Folding(source = "iOpt", destination = "iAny", aggregator = Summarized.Aggregator.ANY),
      new Summarized.Folding(source = "jOpt", destination = "jAny", aggregator = Summarized.Aggregator.ANY),
      new Summarized.Folding(source = "fOpt", destination = "fAny", aggregator = Summarized.Aggregator.ANY),
      new Summarized.Folding(source = "dOpt", destination = "dAny", aggregator = Summarized.Aggregator.ANY),
      new Summarized.Folding(source = "decOpt", destination = "decAny", aggregator = Summarized.Aggregator.ANY),
      new Summarized.Folding(source = "strOpt", destination = "strAny", aggregator = Summarized.Aggregator.ANY),
      new Summarized.Folding(source = "dateOpt", destination = "dateAny", aggregator = Summarized.Aggregator.ANY),
      new Summarized.Folding(source = "dtOpt", destination = "dtAny", aggregator = Summarized.Aggregator.ANY),

      new Summarized.Folding(source = "bOpt", destination = "bSum", aggregator = Summarized.Aggregator.SUM),
      new Summarized.Folding(source = "sOpt", destination = "sSum", aggregator = Summarized.Aggregator.SUM),
      new Summarized.Folding(source = "iOpt", destination = "iSum", aggregator = Summarized.Aggregator.SUM),
      new Summarized.Folding(source = "jOpt", destination = "jSum", aggregator = Summarized.Aggregator.SUM),
      new Summarized.Folding(source = "fOpt", destination = "fSum", aggregator = Summarized.Aggregator.SUM),
      new Summarized.Folding(source = "dOpt", destination = "dSum", aggregator = Summarized.Aggregator.SUM),
      new Summarized.Folding(source = "decOpt", destination = "decSum", aggregator = Summarized.Aggregator.SUM),

      new Summarized.Folding(source = "zOpt", destination = "zMax", aggregator = Summarized.Aggregator.MAX),
      new Summarized.Folding(source = "bOpt", destination = "bMax", aggregator = Summarized.Aggregator.MAX),
      new Summarized.Folding(source = "sOpt", destination = "sMax", aggregator = Summarized.Aggregator.MAX),
      new Summarized.Folding(source = "iOpt", destination = "iMax", aggregator = Summarized.Aggregator.MAX),
      new Summarized.Folding(source = "jOpt", destination = "jMax", aggregator = Summarized.Aggregator.MAX),
      new Summarized.Folding(source = "fOpt", destination = "fMax", aggregator = Summarized.Aggregator.MAX),
      new Summarized.Folding(source = "dOpt", destination = "dMax", aggregator = Summarized.Aggregator.MAX),
      new Summarized.Folding(source = "decOpt", destination = "decMax", aggregator = Summarized.Aggregator.MAX),
      new Summarized.Folding(source = "strOpt", destination = "strMax", aggregator = Summarized.Aggregator.MAX),
      new Summarized.Folding(source = "dateOpt", destination = "dateMax", aggregator = Summarized.Aggregator.MAX),
      new Summarized.Folding(source = "dtOpt", destination = "dtMax", aggregator = Summarized.Aggregator.MAX),

      new Summarized.Folding(source = "zOpt", destination = "zMin", aggregator = Summarized.Aggregator.MIN),
      new Summarized.Folding(source = "bOpt", destination = "bMin", aggregator = Summarized.Aggregator.MIN),
      new Summarized.Folding(source = "sOpt", destination = "sMin", aggregator = Summarized.Aggregator.MIN),
      new Summarized.Folding(source = "iOpt", destination = "iMin", aggregator = Summarized.Aggregator.MIN),
      new Summarized.Folding(source = "jOpt", destination = "jMin", aggregator = Summarized.Aggregator.MIN),
      new Summarized.Folding(source = "fOpt", destination = "fMin", aggregator = Summarized.Aggregator.MIN),
      new Summarized.Folding(source = "dOpt", destination = "dMin", aggregator = Summarized.Aggregator.MIN),
      new Summarized.Folding(source = "decOpt", destination = "decMin", aggregator = Summarized.Aggregator.MIN),
      new Summarized.Folding(source = "strOpt", destination = "strMin", aggregator = Summarized.Aggregator.MIN),
      new Summarized.Folding(source = "dateOpt", destination = "dateMin", aggregator = Summarized.Aggregator.MIN),
      new Summarized.Folding(source = "dtOpt", destination = "dtMin", aggregator = Summarized.Aggregator.MIN),

      new Summarized.Folding(source = "key", destination = "count", aggregator = Summarized.Aggregator.COUNT))))
  class SummarizedValue extends DataModel[SummarizedValue] {

    val key = new IntOption()

    val zAny = new BooleanOption()
    val bAny = new ByteOption()
    val sAny = new ShortOption()
    val iAny = new IntOption()
    val jAny = new LongOption()
    val fAny = new FloatOption()
    val dAny = new DoubleOption()
    val decAny = new DecimalOption()
    val strAny = new StringOption()
    val dateAny = new DateOption()
    val dtAny = new DateTimeOption()

    val bSum = new LongOption()
    val sSum = new LongOption()
    val iSum = new LongOption()
    val jSum = new LongOption()
    val fSum = new DoubleOption()
    val dSum = new DoubleOption()
    val decSum = new DecimalOption()

    val zMax = new BooleanOption()
    val bMax = new ByteOption()
    val sMax = new ShortOption()
    val iMax = new IntOption()
    val jMax = new LongOption()
    val fMax = new FloatOption()
    val dMax = new DoubleOption()
    val decMax = new DecimalOption()
    val strMax = new StringOption()
    val dateMax = new DateOption()
    val dtMax = new DateTimeOption()

    val zMin = new BooleanOption()
    val bMin = new ByteOption()
    val sMin = new ShortOption()
    val iMin = new IntOption()
    val jMin = new LongOption()
    val fMin = new FloatOption()
    val dMin = new DoubleOption()
    val decMin = new DecimalOption()
    val strMin = new StringOption()
    val dateMin = new DateOption()
    val dtMin = new DateTimeOption()

    val count = new LongOption()

    override def reset(): Unit = {
      key.setNull()

      zAny.setNull()
      bAny.setNull()
      sAny.setNull()
      iAny.setNull()
      jAny.setNull()
      fAny.setNull()
      dAny.setNull()
      decAny.setNull()
      strAny.setNull()
      dateAny.setNull()
      dtAny.setNull()

      bSum.setNull()
      sSum.setNull()
      iSum.setNull()
      jSum.setNull()
      fSum.setNull()
      dSum.setNull()
      decSum.setNull()

      zMax.setNull()
      bMax.setNull()
      sMax.setNull()
      iMax.setNull()
      jMax.setNull()
      fMax.setNull()
      dMax.setNull()
      decMax.setNull()
      strMax.setNull()
      dateMax.setNull()
      dtMax.setNull()

      zMin.setNull()
      bMin.setNull()
      sMin.setNull()
      iMin.setNull()
      jMin.setNull()
      fMin.setNull()
      dMin.setNull()
      decMin.setNull()
      strMin.setNull()
      dateMin.setNull()
      dtMin.setNull()

      count.setNull()
    }

    override def copyFrom(other: SummarizedValue): Unit = {
      key.copyFrom(other.key)

      zAny.copyFrom(other.zAny)
      bAny.copyFrom(other.bAny)
      sAny.copyFrom(other.sAny)
      iAny.copyFrom(other.iAny)
      jAny.copyFrom(other.jAny)
      fAny.copyFrom(other.fAny)
      dAny.copyFrom(other.dAny)
      decAny.copyFrom(other.decAny)
      strAny.copyFrom(other.strAny)
      dateAny.copyFrom(other.dateAny)
      dtAny.copyFrom(other.dtAny)

      bSum.copyFrom(other.bSum)
      sSum.copyFrom(other.sSum)
      iSum.copyFrom(other.iSum)
      jSum.copyFrom(other.jSum)
      fSum.copyFrom(other.fSum)
      dSum.copyFrom(other.dSum)
      decSum.copyFrom(other.decSum)

      zMax.copyFrom(other.zMax)
      bMax.copyFrom(other.bMax)
      sMax.copyFrom(other.sMax)
      iMax.copyFrom(other.iMax)
      jMax.copyFrom(other.jMax)
      fMax.copyFrom(other.fMax)
      dMax.copyFrom(other.dMax)
      decMax.copyFrom(other.decMax)
      strMax.copyFrom(other.strMax)
      dateMax.copyFrom(other.dateMax)
      dtMax.copyFrom(other.dtMax)

      zMin.copyFrom(other.zMin)
      bMin.copyFrom(other.bMin)
      sMin.copyFrom(other.sMin)
      iMin.copyFrom(other.iMin)
      jMin.copyFrom(other.jMin)
      fMin.copyFrom(other.fMin)
      dMin.copyFrom(other.dMin)
      decMin.copyFrom(other.decMin)
      strMin.copyFrom(other.strMin)
      dateMin.copyFrom(other.dateMin)
      dtMin.copyFrom(other.dtMin)

      count.copyFrom(other.count)
    }

    def getKeyOption: IntOption = key

    def getZAnyOption = zAny
    def getBAnyOption = bAny
    def getSAnyOption = sAny
    def getIAnyOption = iAny
    def getJAnyOption = jAny
    def getFAnyOption = fAny
    def getDAnyOption = dAny
    def getDecAnyOption = decAny
    def getStrAnyOption = strAny
    def getDateAnyOption = dateAny
    def getDtAnyOption = dtAny

    def getBSumOption = bSum
    def getSSumOption = sSum
    def getISumOption = iSum
    def getJSumOption = jSum
    def getFSumOption = fSum
    def getDSumOption = dSum
    def getDecSumOption = decSum

    def getZMaxOption = zMax
    def getBMaxOption = bMax
    def getSMaxOption = sMax
    def getIMaxOption = iMax
    def getJMaxOption = jMax
    def getFMaxOption = fMax
    def getDMaxOption = dMax
    def getDecMaxOption = decMax
    def getStrMaxOption = strMax
    def getDateMaxOption = dateMax
    def getDtMaxOption = dtMax

    def getZMinOption = zMin
    def getBMinOption = bMin
    def getSMinOption = sMin
    def getIMinOption = iMin
    def getJMinOption = jMin
    def getFMinOption = fMin
    def getDMinOption = dMin
    def getDecMinOption = decMin
    def getStrMinOption = strMin
    def getDateMinOption = dateMin
    def getDtMinOption = dtMin

    def getCountOption = count
  }

  abstract class SummarizeOperator {

    @Summarize(partialAggregation = PartialAggregation.PARTIAL)
    def summarize(value: Value): SummarizedValue
  }
}
