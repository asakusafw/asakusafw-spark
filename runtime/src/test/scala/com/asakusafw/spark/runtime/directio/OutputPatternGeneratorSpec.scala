/*
 * Copyright 2011-2019 Asakusa Framework Team.
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
package com.asakusafw.spark.runtime
package directio

import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner

import java.math.{ BigDecimal => JBigDecimal }
import java.util.Random

import com.asakusafw.runtime.value._
import com.asakusafw.spark.runtime.directio.OutputPatternGenerator._

@RunWith(classOf[JUnitRunner])
class OutputPatternGeneratorSpecTest extends OutputPatternGeneratorSpec

class OutputPatternGeneratorSpec extends FlatSpec with StageInfoSugar {

  behavior of classOf[OutputPatternGenerator[_]].getSimpleName

  import OutputPatternGeneratorSpec._

  it should "generate simple" in {
    val generator = new FooOutputPatternGenerator(Seq(constant("hello")))
    assert(generator.generate(new Foo())(newStageInfo()).getAsString === "hello")
  }

  it should "generate simple with batch argument" in {
    val generator = new FooOutputPatternGenerator(Seq(constant("hello_${arg}")))
    val stageInfo = newStageInfo(batchArguments = Map("arg" -> "bar"))
    assert(generator.generate(new Foo())(stageInfo).getAsString === "hello_bar")
  }

  it should "generate random" in {
    val generator = new FooOutputPatternGenerator(Seq(
      constant("r-"),
      random(0xcafebabe, 1, 9)))

    val rnd = new Random(0xcafebabe)
    for (_ <- 0 until 100) {
      assert(generator.generate(new Foo())(newStageInfo()).getAsString
        === s"r-${rnd.nextInt(9 - 1 + 1) + 1}")
    }
  }

  it should "generate random with batch argument" in {
    val generator = new FooOutputPatternGenerator(Seq(
      constant("r-${arg}-"),
      random(0xcafebabe, 1, 9)))
    val stageInfo = newStageInfo(batchArguments = Map("arg" -> "bar"))

    val rnd = new Random(0xcafebabe)
    for (_ <- 0 until 100) {
      assert(generator.generate(new Foo())(stageInfo).getAsString
        === s"r-bar-${rnd.nextInt(9 - 1 + 1) + 1}")
    }
  }

  it should "generate natural" in {
    val generator = new FooOutputPatternGenerator(Seq(constant("p-"), natural("str")))
    assert(generator.generate(new Foo(_str = Some("v")))(newStageInfo()).getAsString
      === "p-v")
  }

  it should "generate natural with batch argument" in {
    val generator = new FooOutputPatternGenerator(Seq(constant("p-${arg}-"), natural("str")))
    val stageInfo = newStageInfo(batchArguments = Map("arg" -> "bar"))
    assert(generator.generate(new Foo(_str = Some("${arg}")))(stageInfo).getAsString
      === "p-bar-${arg}")
  }

  it should "generate byte format" in {
    val generator = new FooOutputPatternGenerator(Seq(
      constant("p-"),
      byte("byte", "0000")))
    assert(generator.generate(new Foo(_byte = Some(1)))(newStageInfo()).getAsString
      === "p-0001")
  }

  it should "generate byte format with batch argument" in {
    val generator = new FooOutputPatternGenerator(Seq(
      constant("p-${arg}-"),
      byte("byte", "0000")))
    val stageInfo = newStageInfo(batchArguments = Map("arg" -> "bar"))
    assert(generator.generate(new Foo(_byte = Some(1)))(stageInfo).getAsString
      === "p-bar-0001")
  }

  it should "generate short format" in {
    val generator = new FooOutputPatternGenerator(Seq(
      constant("p-"),
      short("short", "0000")))
    assert(generator.generate(new Foo(_short = Some(1)))(newStageInfo()).getAsString
      === "p-0001")
  }

  it should "generate short format with batch argument" in {
    val generator = new FooOutputPatternGenerator(Seq(
      constant("p-${arg}-"),
      short("short", "0000")))
    val stageInfo = newStageInfo(batchArguments = Map("arg" -> "bar"))
    assert(generator.generate(new Foo(_short = Some(1)))(stageInfo).getAsString
      === "p-bar-0001")
  }

  it should "generate int format" in {
    val generator = new FooOutputPatternGenerator(Seq(
      constant("p-"),
      int("int", "0000")))
    assert(generator.generate(new Foo(_int = Some(1)))(newStageInfo()).getAsString
      === "p-0001")
  }

  it should "generate int format with batch argument" in {
    val generator = new FooOutputPatternGenerator(Seq(
      constant("p-${arg}-"),
      int("int", "0000")))
    val stageInfo = newStageInfo(batchArguments = Map("arg" -> "bar"))
    assert(generator.generate(new Foo(_int = Some(1)))(stageInfo).getAsString
      === "p-bar-0001")
  }

  it should "generate long format" in {
    val generator = new FooOutputPatternGenerator(Seq(
      constant("p-"),
      long("long", "0000")))
    assert(generator.generate(new Foo(_long = Some(1)))(newStageInfo()).getAsString
      === "p-0001")
  }

  it should "generate long format with batch argument" in {
    val generator = new FooOutputPatternGenerator(Seq(
      constant("p-${arg}-"),
      long("long", "0000")))
    val stageInfo = newStageInfo(batchArguments = Map("arg" -> "bar"))
    assert(generator.generate(new Foo(_long = Some(1)))(stageInfo).getAsString
      === "p-bar-0001")
  }

  it should "generate float format" in {
    val generator = new FooOutputPatternGenerator(Seq(
      constant("p-"),
      float("float", "0000")))
    assert(generator.generate(new Foo(_float = Some(1)))(newStageInfo()).getAsString
      === "p-0001")
  }

  it should "generate float format with batch argument" in {
    val generator = new FooOutputPatternGenerator(Seq(
      constant("p-${arg}-"),
      float("float", "0000")))
    val stageInfo = newStageInfo(batchArguments = Map("arg" -> "bar"))
    assert(generator.generate(new Foo(_float = Some(1)))(stageInfo).getAsString
      === "p-bar-0001")
  }

  it should "generate double format" in {
    val generator = new FooOutputPatternGenerator(Seq(
      constant("p-"),
      double("double", "0000")))
    assert(generator.generate(new Foo(_double = Some(1)))(newStageInfo()).getAsString
      === "p-0001")
  }

  it should "generate double format with batch argument" in {
    val generator = new FooOutputPatternGenerator(Seq(
      constant("p-${arg}-"),
      double("double", "0000")))
    val stageInfo = newStageInfo(batchArguments = Map("arg" -> "bar"))
    assert(generator.generate(new Foo(_double = Some(1)))(stageInfo).getAsString
      === "p-bar-0001")
  }

  it should "generate decimal format" in {
    val generator = new FooOutputPatternGenerator(Seq(
      constant("p-"),
      decimal("decimal", "0000")))
    assert(generator.generate(new Foo(_decimal = Some(JBigDecimal.valueOf(1))))(newStageInfo()).getAsString
      === "p-0001")
  }

  it should "generate decimal format with batch argument" in {
    val generator = new FooOutputPatternGenerator(Seq(
      constant("p-${arg}-"),
      decimal("decimal", "0000")))
    val stageInfo = newStageInfo(batchArguments = Map("arg" -> "bar"))
    assert(generator.generate(new Foo(_decimal = Some(JBigDecimal.valueOf(1))))(stageInfo).getAsString
      === "p-bar-0001")
  }

  it should "generate date format" in {
    val generator = new FooOutputPatternGenerator(Seq(
      constant("p-"),
      date("date", "yyyyMMdd")))
    assert(generator.generate(new Foo(_date = Some(new Date(2000, 1, 2))))(newStageInfo()).getAsString
      === "p-20000102")
  }

  it should "generate date format with batch argument" in {
    val generator = new FooOutputPatternGenerator(Seq(
      constant("p-${arg}-"),
      date("date", "yyyyMMdd")))
    val stageInfo = newStageInfo(batchArguments = Map("arg" -> "bar"))
    assert(generator.generate(new Foo(_date = Some(new Date(2000, 1, 2))))(stageInfo).getAsString
      === "p-bar-20000102")
  }

  it should "generate datetime format" in {
    val generator = new FooOutputPatternGenerator(Seq(
      constant("p-"),
      datetime("dateTime", "yyyyMMdd")))
    assert(generator.generate(
      new Foo(_dateTime = Some(new DateTime(2000, 1, 2, 3, 4, 5))))(newStageInfo()).getAsString
      === "p-20000102")
  }

  it should "generate datetime format with batch argument" in {
    val generator = new FooOutputPatternGenerator(Seq(
      constant("p-${arg}-"),
      datetime("dateTime", "yyyyMMdd")))
    val stageInfo = newStageInfo(batchArguments = Map("arg" -> "bar"))
    assert(generator.generate(
      new Foo(_dateTime = Some(new DateTime(2000, 1, 2, 3, 4, 5))))(stageInfo).getAsString
      === "p-bar-20000102")
  }
}

object OutputPatternGeneratorSpec {

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
    _dateTime: Option[DateTime] = None) {

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
  }

  class FooOutputPatternGenerator(fragments: Seq[Fragment])
    extends OutputPatternGenerator[Foo](fragments) {

    override def getProperty(foo: Foo, property: String): ValueOption[_] = {
      property match {
        case "str" => foo.str
        case "byte" => foo.byte
        case "short" => foo.short
        case "int" => foo.int
        case "long" => foo.long
        case "float" => foo.float
        case "double" => foo.double
        case "decimal" => foo.decimal
        case "date" => foo.date
        case "dateTime" => foo.dateTime
      }
    }
  }
}
