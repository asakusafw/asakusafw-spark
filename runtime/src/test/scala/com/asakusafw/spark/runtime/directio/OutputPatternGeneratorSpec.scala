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
package com.asakusafw.spark.runtime.directio

import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner

import java.util.Random

import com.asakusafw.runtime.value._
import com.asakusafw.spark.runtime.directio.OutputPatternGenerator._

@RunWith(classOf[JUnitRunner])
class OutputPatternGeneratorSpecTest extends OutputPatternGeneratorSpec

class OutputPatternGeneratorSpec extends FlatSpec {

  behavior of classOf[OutputPatternGenerator[_]].getSimpleName

  import OutputPatternGeneratorSpec._

  it should "generate simple" in {
    val generator = new FooOutputPatternGenerator(Seq(constant("hello")))
    assert(generator.generate(new Foo()).getAsString === "hello")
  }

  it should "generate random" in {
    val generator = new FooOutputPatternGenerator(Seq(
      constant("r-"),
      random(0xcafebabe, 1, 9)))

    val rnd = new Random(0xcafebabe)
    for (_ <- 0 until 100) {
      assert(generator.generate(new Foo()).getAsString === s"r-${rnd.nextInt(9 - 1 + 1) + 1}")
    }
  }

  it should "generate natural" in {
    val generator = new FooOutputPatternGenerator(Seq(constant("p-"), natural("str")))
    assert(generator.generate(new Foo(_str = Some("v"))).getAsString === "p-v")
  }

  it should "generate date format" in {
    val generator = new FooOutputPatternGenerator(Seq(
      constant("p-"),
      date("date", "yyyyMMdd")))
    assert(generator.generate(new Foo(_date = Some(new Date(2000, 1, 2)))).getAsString
      === "p-20000102")
  }

  it should "generate datetime format" in {
    val generator = new FooOutputPatternGenerator(Seq(
      constant("p-"),
      dateTime("dateTime", "yyyyMMdd")))
    assert(generator.generate(
      new Foo(_dateTime = Some(new DateTime(2000, 1, 2, 3, 4, 5)))).getAsString
      === "p-20000102")
  }
}

object OutputPatternGeneratorSpec {

  class Foo(
    _str: Option[String] = None,
    _date: Option[Date] = None,
    _dateTime: Option[DateTime] = None) {

    val str: StringOption = new StringOption()
    val date: DateOption = new DateOption()
    val dateTime: DateTimeOption = new DateTimeOption()

    _str.foreach(str.modify)
    _date.foreach(date.modify)
    _dateTime.foreach(dateTime.modify)
  }

  class FooOutputPatternGenerator(fragments: Seq[Fragment])
    extends OutputPatternGenerator[Foo](fragments) {

    override def getProperty(foo: Foo, property: String): ValueOption[_] = {
      property match {
        case "str" => foo.str
        case "date" => foo.date
        case "dateTime" => foo.dateTime
      }
    }
  }
}
