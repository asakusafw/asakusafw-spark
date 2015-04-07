package com.asakusafw.spark.compiler
package subplan

import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner

import scala.collection.mutable

import com.asakusafw.spark.runtime.driver.BranchKey
import com.asakusafw.spark.tools.asm._

@RunWith(classOf[JUnitRunner])
class BranchKeysClassBuilderSpecTest extends BranchKeysClassBuilderSpec

class BranchKeysClassBuilderSpec extends FlatSpec with LoadClassSugar {

  behavior of classOf[BranchKeysClassBuilder].getSimpleName

  it should "compile BranchKeys" in {
    val builder = new BranchKeysClassBuilder("flowId")
    val branch0 = builder.getField(10L)
    val branch1 = builder.getField(11L)
    val cls = loadClass(builder.thisType.getClassName, builder.build())

    val branchKeys = cls.getDeclaredFields().sortBy(_.getName)
    assert(branchKeys.size === 2)
    assert(branchKeys(0).getName === branch0)
    assert(branchKeys(0).get(null) === BranchKey(0))
    assert(branchKeys(1).getName === branch1)
    assert(branchKeys(1).get(null) === BranchKey(1))
  }
}
