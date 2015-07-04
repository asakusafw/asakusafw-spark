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
package subplan

import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner

import java.lang.reflect.InvocationTargetException

import scala.collection.mutable

import com.asakusafw.spark.runtime.rdd.BranchKey
import com.asakusafw.spark.tools.asm._

@RunWith(classOf[JUnitRunner])
class BranchKeysClassBuilderSpecTest extends BranchKeysClassBuilderSpec

class BranchKeysClassBuilderSpec extends FlatSpec with LoadClassSugar {

  behavior of classOf[BranchKeysClassBuilder].getSimpleName

  it should "compile BranchKeys" in {
    val builder = new BranchKeysClassBuilder("flowId")
    val branch0 = builder.getField(10L)
    val branch1 = builder.getField(11L)
    builder.getField(12L)
    builder.getField(13L)
    builder.getField(14L)
    builder.getField(15L)
    val cls = loadClass(builder.thisType.getClassName, builder.build())

    val branchKeys = cls.getDeclaredFields().filter(_.getName.startsWith("BRANCH_")).sortBy(_.getName)
    assert(branchKeys.size === 6)
    assert(branchKeys(0).getName === branch0)
    assert(branchKeys(0).get(null) === BranchKey(0))
    assert(branchKeys(1).getName === branch1)
    assert(branchKeys(1).get(null) === BranchKey(1))

    val valueOf = cls.getMethod("valueOf", classOf[Int])

    for (i <- 0 until branchKeys.size) {
      assert(valueOf.invoke(null, Int.box(i)) === BranchKey(i))
    }

    intercept[IllegalArgumentException] {
      try {
        valueOf.invoke(null, Int.box(-1))
      } catch {
        case e: InvocationTargetException => throw e.getCause
      }
    }
    intercept[IllegalArgumentException] {
      try {
        valueOf.invoke(null, Int.box(branchKeys.size))
      } catch {
        case e: InvocationTargetException => throw e.getCause
      }
    }
  }
}
