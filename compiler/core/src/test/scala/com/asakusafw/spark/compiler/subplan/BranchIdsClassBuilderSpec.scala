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
    builder.getField(12L)
    builder.getField(13L)
    builder.getField(14L)
    builder.getField(15L)
    val cls = loadClass(builder.thisType.getClassName, builder.build())

    val broadcastIds = cls.getDeclaredFields().filter(_.getName.startsWith("BROADCAST_")).sortBy(_.getName)
    assert(broadcastIds.size === 6)
    assert(broadcastIds(0).getName === branch0)
    assert(broadcastIds(0).get(null) === BroadcastId(0))
    assert(broadcastIds(1).getName === branch1)
    assert(broadcastIds(1).get(null) === BroadcastId(1))

    for (i <- 0 until broadcastIds.size) {
      assert(cls.getMethod("valueOf", classOf[Int]).invoke(null, Int.box(i)) === BroadcastId(i))
    }
  }
}
