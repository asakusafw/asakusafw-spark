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
package directio

import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner

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
      dateTime("dateTime", "yyyyMMdd")))
    assert(generator.generate(
      new Foo(_dateTime = Some(new DateTime(2000, 1, 2, 3, 4, 5))))(newStageInfo()).getAsString
      === "p-20000102")
  }

  it should "compile OutputPatternGenerator datetime format with batch argument" in {
    val generator = newGenerator(Seq(
      constant("p-${arg}-"),
      dateTime("dateTime", "yyyyMMdd")))
    val stageInfo = newStageInfo(batchArguments = Map("arg" -> "bar"))
    assert(generator.generate(
      new Foo(_dateTime = Some(new DateTime(2000, 1, 2, 3, 4, 5))))(stageInfo).getAsString
      === "p-bar-20000102")
  }
}

object OutputPatternGeneratorClassBuilderSpec {

  class Foo(
    _str: Option[String] = None,
    _date: Option[Date] = None,
    _dateTime: Option[DateTime] = None) extends DataModel[Foo] {

    val str: StringOption = new StringOption()
    val date: DateOption = new DateOption()
    val dateTime: DateTimeOption = new DateTimeOption()

    _str.foreach(str.modify)
    _date.foreach(date.modify)
    _dateTime.foreach(dateTime.modify)

    override def reset(): Unit = {
      str.setNull()
      date.setNull()
      dateTime.setNull()
    }
    override def copyFrom(other: Foo): Unit = {
      str.copyFrom(other.str)
      date.copyFrom(other.date)
      dateTime.copyFrom(other.dateTime)
    }

    def getStrOption(): StringOption = str
    def getDateOption(): DateOption = date
    def getDateTimeOption(): DateTimeOption = dateTime
  }
}
