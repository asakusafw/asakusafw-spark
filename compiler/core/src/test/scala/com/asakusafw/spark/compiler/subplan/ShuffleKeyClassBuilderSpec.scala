package com.asakusafw.spark.compiler
package subplan

import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner

import com.asakusafw.runtime.model.DataModel
import com.asakusafw.runtime.value._
import com.asakusafw.spark.runtime.driver.ShuffleKey
import com.asakusafw.spark.tools.asm._

@RunWith(classOf[JUnitRunner])
class ShuffleKeyClassBuilderSpecTest extends ShuffleKeyClassBuilderSpec

class ShuffleKeyClassBuilderSpec extends FlatSpec with LoadClassSugar {

  import ShuffleKeyClassBuilderSpec._

  behavior of classOf[ShuffleKeyClassBuilder].getSimpleName

  it should "compile ShuffleKey" in {
    val builder = new ShuffleKeyClassBuilder(
      "flowId",
      classOf[Hoge].asType,
      Seq(("i", classOf[IntOption].asType)),
      Seq(("l", classOf[LongOption].asType)))
    val cls = loadClass(builder.thisType.getClassName, builder.build()).asSubclass(classOf[ShuffleKey])

    val hoge = new Hoge()
    hoge.i.modify(10)
    hoge.l.modify(100)
    hoge.s.modify("hoge")

    {
      val shuffleKey = cls.newInstance()
      assert(shuffleKey.grouping.size === 1)
      assert(shuffleKey.grouping(0).getClass === classOf[IntOption])
      assert(shuffleKey.grouping(0).isNull)

      assert(shuffleKey.ordering.size === 1)
      assert(shuffleKey.ordering(0).getClass === classOf[LongOption])
      assert(shuffleKey.ordering(0).isNull)

      cls.getMethod("copyFrom", classOf[Hoge]).invoke(shuffleKey, hoge)

      assert(shuffleKey.grouping(0).asInstanceOf[IntOption].get === 10)
      assert(shuffleKey.ordering(0).asInstanceOf[LongOption].get === 100)
    }

    {
      val shuffleKey = cls.getConstructor(classOf[Hoge]).newInstance(hoge)

      assert(shuffleKey.grouping.size === 1)
      assert(shuffleKey.grouping(0).getClass === classOf[IntOption])
      assert(shuffleKey.grouping(0).asInstanceOf[IntOption].get === 10)

      assert(shuffleKey.ordering.size === 1)
      assert(shuffleKey.ordering(0).getClass === classOf[LongOption])
      assert(shuffleKey.ordering(0).asInstanceOf[LongOption].get === 100)
    }
  }
}

object ShuffleKeyClassBuilderSpec {

  class Hoge extends DataModel[Hoge] {

    val i: IntOption = new IntOption()
    val l: LongOption = new LongOption()
    val s: StringOption = new StringOption()

    override def reset(): Unit = {
      i.setNull()
      l.setNull()
      s.setNull()
    }

    override def copyFrom(other: Hoge): Unit = {
      i.copyFrom(other.i)
      l.copyFrom(other.l)
      s.copyFrom(other.s)
    }
  }
}
