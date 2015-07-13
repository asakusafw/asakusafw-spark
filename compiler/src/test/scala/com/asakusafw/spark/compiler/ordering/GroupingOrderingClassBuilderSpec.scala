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
package ordering

import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner

import com.asakusafw.runtime.value._
import com.asakusafw.spark.runtime.driver.ShuffleKey
import com.asakusafw.spark.runtime.io.WritableSerDe
import com.asakusafw.spark.tools.asm._

@RunWith(classOf[JUnitRunner])
class GroupingOrderingClassBuilderSpecTest extends GroupingOrderingClassBuilderSpec

class GroupingOrderingClassBuilderSpec extends FlatSpec with LoadClassSugar with TempDir with CompilerContext {

  behavior of classOf[GroupingOrderingClassBuilder].getSimpleName

  it should "compile grouping ordering" in {
    val classpath = createTempDirectory("SortOrderingClassBuilderSpec").toFile
    implicit val context = newContext("flowId", classpath)

    val builder = new GroupingOrderingClassBuilder(
      Seq(classOf[IntOption].asType, classOf[LongOption].asType))
    val cls = loadClass(builder.thisType.getClassName, builder.build()).asSubclass(classOf[Ordering[ShuffleKey]])
    val ordering = cls.newInstance()

    {
      val x = new ShuffleKey(
        WritableSerDe.serialize(Seq(new IntOption().modify(1), new LongOption().modify(1L))), Array.empty)
      val y = new ShuffleKey(
        WritableSerDe.serialize(Seq(new IntOption().modify(1), new LongOption().modify(1L))), Array.empty)
      assert(ordering.equiv(x, y) === true)
      assert(ordering.lteq(x, y) === true)
      assert(ordering.lt(x, y) === false)
      assert(ordering.gteq(x, y) === true)
      assert(ordering.gt(x, y) === false)
    }
    {
      val x = new ShuffleKey(
        WritableSerDe.serialize(Seq(new IntOption().modify(0), new LongOption().modify(1L))), Array.empty)
      val y = new ShuffleKey(
        WritableSerDe.serialize(Seq(new IntOption().modify(1), new LongOption().modify(1L))), Array.empty)
      assert(ordering.equiv(x, y) === false)
      assert(ordering.lteq(x, y) === true)
      assert(ordering.lt(x, y) === true)
      assert(ordering.gteq(x, y) === false)
      assert(ordering.gt(x, y) === false)
    }
    {
      val x = new ShuffleKey(
        WritableSerDe.serialize(Seq(new IntOption().modify(1), new LongOption().modify(1L))), Array.empty)
      val y = new ShuffleKey(
        WritableSerDe.serialize(Seq(new IntOption().modify(0), new LongOption().modify(1L))), Array.empty)
      assert(ordering.equiv(x, y) === false)
      assert(ordering.lteq(x, y) === false)
      assert(ordering.lt(x, y) === false)
      assert(ordering.gteq(x, y) === true)
      assert(ordering.gt(x, y) === true)
    }
    {
      val x = new ShuffleKey(
        WritableSerDe.serialize(Seq(new IntOption().modify(1), new LongOption().modify(0L))), Array.empty)
      val y = new ShuffleKey(
        WritableSerDe.serialize(Seq(new IntOption().modify(1), new LongOption().modify(1L))), Array.empty)
      assert(ordering.equiv(x, y) === false)
      assert(ordering.lteq(x, y) === true)
      assert(ordering.lt(x, y) === true)
      assert(ordering.gteq(x, y) === false)
      assert(ordering.gt(x, y) === false)
    }
    {
      val x = new ShuffleKey(
        WritableSerDe.serialize(Seq(new IntOption().modify(1), new LongOption().modify(1L))), Array.empty)
      val y = new ShuffleKey(
        WritableSerDe.serialize(Seq(new IntOption().modify(1), new LongOption().modify(0L))), Array.empty)
      assert(ordering.equiv(x, y) === false)
      assert(ordering.lteq(x, y) === false)
      assert(ordering.lt(x, y) === false)
      assert(ordering.gteq(x, y) === true)
      assert(ordering.gt(x, y) === true)
    }
  }
}
