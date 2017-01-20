/*
 * Copyright 2011-2017 Asakusa Framework Team.
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
package directio

import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner

import java.math.{ BigDecimal => JBigDecimal }
import java.util.Random

import com.asakusafw.lang.compiler.api.testing.MockDataModelLoader
import com.asakusafw.lang.compiler.model.description.ClassDescription
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.runtime.value._
import com.asakusafw.spark.compiler.directio.OutputPatternGeneratorClassBuilderSpec._
import com.asakusafw.spark.runtime.StageInfoSugar
import com.asakusafw.spark.runtime.directio.OutputPatternGenerator
import com.asakusafw.spark.runtime.directio.OutputPatternGenerator._

@RunWith(classOf[JUnitRunner])
class OutputPatternGeneratorClassBuilderSpecTest extends OutputPatternGeneratorClassBuilderSpec

class OutputPatternGeneratorClassBuilderSpec
  extends FlatSpec
  with FlowIdForEach
  with StageInfoSugar
  with UsingCompilerContext {

  behavior of classOf[OutputPatternGeneratorClassBuilder].getSimpleName

  def newGenerator(fragments: Seq[Fragment]): OutputPatternGenerator[Foo] = {
    implicit val context = newCompilerContext(flowId)
    val dataModelRef = MockDataModelLoader.load(context.cl, ClassDescription.of(classOf[Foo]))

    val generatorType = OutputPatternGeneratorClassBuilder.getOrCompile(dataModelRef)
    context.loadClass(generatorType.getClassName)
      .getConstructor(classOf[Seq[Fragment]])
      .newInstance(fragments)
  }

  it should "compile OutputPatternGenerator simple" in {
    val generator = newGenerator(Seq(constant("hello")))
    assert(generator.generate(new Foo())(newStageInfo()).getAsString
      === "hello")
  }

  it should "compile OutputPatternGenerator simple with batch argument" in {
    val generator = newGenerator(Seq(constant("hello-${arg}")))
    val stageInfo = newStageInfo(batchArguments = Map("arg" -> "bar"))
    assert(generator.generate(new Foo())(stageInfo).getAsString
      === "hello-bar")
  }

  it should "compile OutputPatternGenerator random" in {
    val generator = newGenerator(Seq(
      constant("r-"),
      random(0xcafebabe, 1, 9)))

    val rnd = new Random(0xcafebabe)
    for (_ <- 0 until 100) {
      assert(generator.generate(new Foo())(newStageInfo()).getAsString
        === s"r-${rnd.nextInt(9 - 1 + 1) + 1}")
    }
  }

  it should "compile OutputPatternGenerator random with batch argument" in {
    val generator = newGenerator(Seq(
      constant("r-${arg}-"),
      random(0xcafebabe, 1, 9)))
    val stageInfo = newStageInfo(batchArguments = Map("arg" -> "bar"))

    val rnd = new Random(0xcafebabe)
    for (_ <- 0 until 100) {
      assert(generator.generate(new Foo())(stageInfo).getAsString
        === s"r-bar-${rnd.nextInt(9 - 1 + 1) + 1}")
    }
  }

  it should "compile OutputPatternGenerator natural" in {
    val generator = newGenerator(Seq(constant("p-"), natural("str")))
    assert(generator.generate(new Foo(_str = Some("v")))(newStageInfo()).getAsString
      === "p-v")
  }

  it should "compile OutputPatternGenerator natural with batch argument" in {
    val generator = newGenerator(Seq(constant("p-${arg}-"), natural("str")))
    val stageInfo = newStageInfo(batchArguments = Map("arg" -> "bar"))
    assert(generator.generate(new Foo(_str = Some("${arg}")))(stageInfo).getAsString
      === "p-bar-${arg}")
  }

  it should "compile OutputPatternGenerator byte format" in {
    val generator = newGenerator(Seq(
      constant("p-"),
      byte("byte", "0000")))
    assert(generator.generate(new Foo(_byte = Some(1)))(newStageInfo()).getAsString
      === "p-0001")
  }

  it should "compile OutputPatternGenerator byte format with batch argument" in {
    val generator = newGenerator(Seq(
      constant("p-${arg}-"),
      byte("byte", "0000")))
    val stageInfo = newStageInfo(batchArguments = Map("arg" -> "bar"))
    assert(generator.generate(new Foo(_byte = Some(1)))(stageInfo).getAsString
      === "p-bar-0001")
  }

  it should "compile OutputPatternGenerator short format" in {
    val generator = newGenerator(Seq(
      constant("p-"),
      short("short", "0000")))
    assert(generator.generate(new Foo(_short = Some(1)))(newStageInfo()).getAsString
      === "p-0001")
  }

  it should "compile OutputPatternGenerator short format with batch argument" in {
    val generator = newGenerator(Seq(
      constant("p-${arg}-"),
      short("short", "0000")))
    val stageInfo = newStageInfo(batchArguments = Map("arg" -> "bar"))
    assert(generator.generate(new Foo(_short = Some(1)))(stageInfo).getAsString
      === "p-bar-0001")
  }

  it should "compile OutputPatternGenerator int format" in {
    val generator = newGenerator(Seq(
      constant("p-"),
      int("int", "0000")))
    assert(generator.generate(new Foo(_int = Some(1)))(newStageInfo()).getAsString
      === "p-0001")
  }

  it should "compile OutputPatternGenerator int format with batch argument" in {
    val generator = newGenerator(Seq(
      constant("p-${arg}-"),
      int("int", "0000")))
    val stageInfo = newStageInfo(batchArguments = Map("arg" -> "bar"))
    assert(generator.generate(new Foo(_int = Some(1)))(stageInfo).getAsString
      === "p-bar-0001")
  }

  it should "compile OutputPatternGenerator long format" in {
    val generator = newGenerator(Seq(
      constant("p-"),
      long("long", "0000")))
    assert(generator.generate(new Foo(_long = Some(1)))(newStageInfo()).getAsString
      === "p-0001")
  }

  it should "compile OutputPatternGenerator long format with batch argument" in {
    val generator = newGenerator(Seq(
      constant("p-${arg}-"),
      long("long", "0000")))
    val stageInfo = newStageInfo(batchArguments = Map("arg" -> "bar"))
    assert(generator.generate(new Foo(_long = Some(1)))(stageInfo).getAsString
      === "p-bar-0001")
  }

  it should "compile OutputPatternGenerator float format" in {
    val generator = newGenerator(Seq(
      constant("p-"),
      float("float", "0000")))
    assert(generator.generate(new Foo(_float = Some(1)))(newStageInfo()).getAsString
      === "p-0001")
  }

  it should "compile OutputPatternGenerator float format with batch argument" in {
    val generator = newGenerator(Seq(
      constant("p-${arg}-"),
      float("float", "0000")))
    val stageInfo = newStageInfo(batchArguments = Map("arg" -> "bar"))
    assert(generator.generate(new Foo(_float = Some(1)))(stageInfo).getAsString
      === "p-bar-0001")
  }

  it should "compile OutputPatternGenerator double format" in {
    val generator = newGenerator(Seq(
      constant("p-"),
      double("double", "0000")))
    assert(generator.generate(new Foo(_double = Some(1)))(newStageInfo()).getAsString
      === "p-0001")
  }

  it should "compile OutputPatternGenerator double format with batch argument" in {
    val generator = newGenerator(Seq(
      constant("p-${arg}-"),
      double("double", "0000")))
    val stageInfo = newStageInfo(batchArguments = Map("arg" -> "bar"))
    assert(generator.generate(new Foo(_double = Some(1)))(stageInfo).getAsString
      === "p-bar-0001")
  }

  it should "compile OutputPatternGenerator decimal format" in {
    val generator = newGenerator(Seq(
      constant("p-"),
      decimal("decimal", "0000")))
    assert(generator.generate(new Foo(_decimal = Some(JBigDecimal.valueOf(1))))(newStageInfo()).getAsString
      === "p-0001")
  }

  it should "compile OutputPatternGenerator decimal format with batch argument" in {
    val generator = newGenerator(Seq(
      constant("p-${arg}-"),
      decimal("decimal", "0000")))
    val stageInfo = newStageInfo(batchArguments = Map("arg" -> "bar"))
    assert(generator.generate(new Foo(_decimal = Some(JBigDecimal.valueOf(1))))(stageInfo).getAsString
      === "p-bar-0001")
  }

  it should "compile OutputPatternGenerator date format" in {
    val generator = newGenerator(Seq(
      constant("p-"),
      date("date", "yyyyMMdd")))
    assert(generator.generate(new Foo(_date = Some(new Date(2000, 1, 2))))(newStageInfo()).getAsString
      === "p-20000102")
  }

  it should "compile OutputPatternGenerator date format with batch argument" in {
    val generator = newGenerator(Seq(
      constant("p-${arg}-"),
      date("date", "yyyyMMdd")))
    val stageInfo = newStageInfo(batchArguments = Map("arg" -> "bar"))
    assert(generator.generate(new Foo(_date = Some(new Date(2000, 1, 2))))(stageInfo).getAsString
      === "p-bar-20000102")
  }

  it should "compile OutputPatternGenerator datetime format" in {
    val generator = newGenerator(Seq(
      constant("p-"),
      datetime("dateTime", "yyyyMMdd")))
    assert(generator.generate(
      new Foo(_dateTime = Some(new DateTime(2000, 1, 2, 3, 4, 5))))(newStageInfo()).getAsString
      === "p-20000102")
  }

  it should "compile OutputPatternGenerator datetime format with batch argument" in {
    val generator = newGenerator(Seq(
      constant("p-${arg}-"),
      datetime("dateTime", "yyyyMMdd")))
    val stageInfo = newStageInfo(batchArguments = Map("arg" -> "bar"))
    assert(generator.generate(
      new Foo(_dateTime = Some(new DateTime(2000, 1, 2, 3, 4, 5))))(stageInfo).getAsString
      === "p-bar-20000102")
  }
}

object OutputPatternGeneratorClassBuilderSpec {

  class Foo(
    _str: Option[String] = None,
    _byte: Option[Byte] = None,
    _short: Option[Short] = None,
    _int: Option[Int] = None,
    _long: Option[Long] = None,
    _float: Option[Float] = None,
    _double: Option[Double] = None,
    _decimal: Option[JBigDecimal] = None,
    _date: Option[Date] = None,
    _dateTime: Option[DateTime] = None) extends DataModel[Foo] {

    val str: StringOption = new StringOption()
    val byte: ByteOption = new ByteOption()
    val short: ShortOption = new ShortOption()
    val int: IntOption = new IntOption()
    val long: LongOption = new LongOption()
    val float: FloatOption = new FloatOption()
    val double: DoubleOption = new DoubleOption()
    val decimal: DecimalOption = new DecimalOption()
    val date: DateOption = new DateOption()
    val dateTime: DateTimeOption = new DateTimeOption()

    _str.foreach(str.modify)
    _byte.foreach(byte.modify)
    _short.foreach(short.modify)
    _int.foreach(int.modify)
    _long.foreach(long.modify)
    _float.foreach(float.modify)
    _double.foreach(double.modify)
    _decimal.foreach(decimal.modify)
    _date.foreach(date.modify)
    _dateTime.foreach(dateTime.modify)

    override def reset(): Unit = {
      str.setNull()
      byte.setNull()
      short.setNull()
      int.setNull()
      long.setNull()
      float.setNull()
      double.setNull()
      decimal.setNull()
      date.setNull()
      dateTime.setNull()
    }
    override def copyFrom(other: Foo): Unit = {
      str.copyFrom(other.str)
      byte.copyFrom(other.byte)
      short.copyFrom(other.short)
      int.copyFrom(other.int)
      long.copyFrom(other.long)
      float.copyFrom(other.float)
      double.copyFrom(other.double)
      decimal.copyFrom(other.decimal)
      date.copyFrom(other.date)
      dateTime.copyFrom(other.dateTime)
    }

    def getStrOption(): StringOption = str
    def getByteOption(): ByteOption = byte
    def getShortOption(): ShortOption = short
    def getIntOption(): IntOption = int
    def getLongOption(): LongOption = long
    def getFloatOption(): FloatOption = float
    def getDoubleOption(): DoubleOption = double
    def getDecimalOption(): DecimalOption = decimal
    def getDateOption(): DateOption = date
    def getDateTimeOption(): DateTimeOption = dateTime
  }
}
