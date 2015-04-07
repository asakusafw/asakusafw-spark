package com.asakusafw.spark.compiler
package subplan

import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner

import scala.collection.mutable

import com.asakusafw.spark.runtime.driver.BroadcastId
import com.asakusafw.spark.tools.asm._

@RunWith(classOf[JUnitRunner])
class BroadcastIdsClassBuilderSpecTest extends BroadcastIdsClassBuilderSpec

class BroadcastIdsClassBuilderSpec extends FlatSpec with LoadClassSugar {

  behavior of classOf[BroadcastIdsClassBuilder].getSimpleName

  it should "compile BroadcastIds" in {
    val builder = new BroadcastIdsClassBuilder("flowId")
    val branch0 = builder.getField(10L)
    val branch1 = builder.getField(11L)
    val cls = loadClass(builder.thisType.getClassName, builder.build())

    val broadcastIds = cls.getDeclaredFields().sortBy(_.getName)
    assert(broadcastIds.size === 2)
    assert(broadcastIds(0).getName === branch0)
    assert(broadcastIds(0).get(null) === BroadcastId(0))
    assert(broadcastIds(1).getName === branch1)
    assert(broadcastIds(1).get(null) === BroadcastId(1))
  }
}
