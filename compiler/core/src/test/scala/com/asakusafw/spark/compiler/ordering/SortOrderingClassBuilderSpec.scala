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
class SortOrderingClassBuilderSpecTest extends SortOrderingClassBuilderSpec

class SortOrderingClassBuilderSpec extends FlatSpec with LoadClassSugar {

  behavior of classOf[SortOrderingClassBuilder].getSimpleName

  it should "compile sort ordering" in {
    val cl = new SimpleClassLoader(Thread.currentThread.getContextClassLoader)
    val grouping = new GroupingOrderingClassBuilder("flowId",
      Seq(classOf[IntOption].asType))
    cl.put(grouping.thisType.getClassName, grouping.build())
    val builder = new SortOrderingClassBuilder("flowId", grouping.thisType,
      Seq((classOf[LongOption].asType, true), (classOf[StringOption].asType, false)))
    cl.put(builder.thisType.getClassName, builder.build())
    val cls = cl.loadClass(builder.thisType.getClassName).asSubclass(classOf[Ordering[ShuffleKey]])
    val ordering = cls.newInstance()

    val serde = new WritableSerDe()

    {
      val x = new ShuffleKey(
        serde.serialize(Seq(new IntOption().modify(1))),
        serde.serialize(Seq(new LongOption().modify(1L), new StringOption().modify("abc"))))
      val y = new ShuffleKey(
        serde.serialize(Seq(new IntOption().modify(1))),
        serde.serialize(Seq(new LongOption().modify(1L), new StringOption().modify("abc"))))
      assert(ordering.equiv(x, y) === true)
      assert(ordering.lteq(x, y) === true)
      assert(ordering.lt(x, y) === false)
      assert(ordering.gteq(x, y) === true)
      assert(ordering.gt(x, y) === false)
    }
    {
      val x = new ShuffleKey(
        serde.serialize(Seq(new IntOption().modify(0))),
        serde.serialize(Seq(new LongOption().modify(1L), new StringOption().modify("abc"))))
      val y = new ShuffleKey(
        serde.serialize(Seq(new IntOption().modify(1))),
        serde.serialize(Seq(new LongOption().modify(1L), new StringOption().modify("abc"))))
      assert(ordering.equiv(x, y) === false)
      assert(ordering.lteq(x, y) === true)
      assert(ordering.lt(x, y) === true)
      assert(ordering.gteq(x, y) === false)
      assert(ordering.gt(x, y) === false)
    }
    {
      val x = new ShuffleKey(
        serde.serialize(Seq(new IntOption().modify(1))),
        serde.serialize(Seq(new LongOption().modify(1L), new StringOption().modify("abc"))))
      val y = new ShuffleKey(
        serde.serialize(Seq(new IntOption().modify(0))),
        serde.serialize(Seq(new LongOption().modify(1L), new StringOption().modify("abc"))))
      assert(ordering.equiv(x, y) === false)
      assert(ordering.lteq(x, y) === false)
      assert(ordering.lt(x, y) === false)
      assert(ordering.gteq(x, y) === true)
      assert(ordering.gt(x, y) === true)
    }
    {
      val x = new ShuffleKey(
        serde.serialize(Seq(new IntOption().modify(1))),
        serde.serialize(Seq(new LongOption().modify(0L), new StringOption().modify("abc"))))
      val y = new ShuffleKey(
        serde.serialize(Seq(new IntOption().modify(1))),
        serde.serialize(Seq(new LongOption().modify(1L), new StringOption().modify("abc"))))
      assert(ordering.equiv(x, y) === false)
      assert(ordering.lteq(x, y) === true)
      assert(ordering.lt(x, y) === true)
      assert(ordering.gteq(x, y) === false)
      assert(ordering.gt(x, y) === false)
    }
    {
      val x = new ShuffleKey(
        serde.serialize(Seq(new IntOption().modify(1))),
        serde.serialize(Seq(new LongOption().modify(1L), new StringOption().modify("abc"))))
      val y = new ShuffleKey(
        serde.serialize(Seq(new IntOption().modify(1))),
        serde.serialize(Seq(new LongOption().modify(0L), new StringOption().modify("abc"))))
      assert(ordering.equiv(x, y) === false)
      assert(ordering.lteq(x, y) === false)
      assert(ordering.lt(x, y) === false)
      assert(ordering.gteq(x, y) === true)
      assert(ordering.gt(x, y) === true)
    }
    {
      val x = new ShuffleKey(
        serde.serialize(Seq(new IntOption().modify(1))),
        serde.serialize(Seq(new LongOption().modify(1L), new StringOption().modify("abd"))))
      val y = new ShuffleKey(
        serde.serialize(Seq(new IntOption().modify(1))),
        serde.serialize(Seq(new LongOption().modify(1L), new StringOption().modify("abc"))))
      assert(ordering.equiv(x, y) === false)
      assert(ordering.lteq(x, y) === true)
      assert(ordering.lt(x, y) === true)
      assert(ordering.gteq(x, y) === false)
      assert(ordering.gt(x, y) === false)
    }
    {
      val x = new ShuffleKey(
        serde.serialize(Seq(new IntOption().modify(1))),
        serde.serialize(Seq(new LongOption().modify(1L), new StringOption().modify("abc"))))
      val y = new ShuffleKey(
        serde.serialize(Seq(new IntOption().modify(1))),
        serde.serialize(Seq(new LongOption().modify(1L), new StringOption().modify("abd"))))
      assert(ordering.equiv(x, y) === false)
      assert(ordering.lteq(x, y) === false)
      assert(ordering.lt(x, y) === false)
      assert(ordering.gteq(x, y) === true)
      assert(ordering.gt(x, y) === true)
    }
  }
}
