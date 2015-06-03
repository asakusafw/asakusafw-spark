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

class GroupingOrderingClassBuilderSpec extends FlatSpec with LoadClassSugar {

  behavior of classOf[GroupingOrderingClassBuilder].getSimpleName

  it should "compile grouping ordering" in {
    val builder = new GroupingOrderingClassBuilder("flowId",
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
