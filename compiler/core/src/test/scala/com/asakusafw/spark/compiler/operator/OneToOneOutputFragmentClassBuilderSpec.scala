package com.asakusafw.spark.compiler.operator

import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner

import java.lang.{ Long => JLong }

import com.asakusafw.runtime.model.DataModel
import com.asakusafw.runtime.value._
import com.asakusafw.spark.runtime.driver.PrepareKey
import com.asakusafw.spark.runtime.fragment.OneToOneOutputFragment
import com.asakusafw.spark.tools.asm._

@RunWith(classOf[JUnitRunner])
class OneToOneOutputFragmentClassBuilderSpecTest extends OneToOneOutputFragmentClassBuilderSpec

class OneToOneOutputFragmentClassBuilderSpec extends FlatSpec with LoadClassSugar {

  import OneToOneOutputFragmentClassBuilderSpec._

  behavior of classOf[OneToOneOutputFragmentClassBuilder].getSimpleName

  it should "compile OutputFragment" in {
    val builder = new OneToOneOutputFragmentClassBuilder(
      "flowId", classOf[String].asType, classOf[TestModel].asType, classOf[IntOption].asType)
    val cls = loadClass(builder.thisType.getClassName, builder.build())
      .asSubclass(classOf[OneToOneOutputFragment[String, TestModel, IntOption]])

    val fragment = cls.getConstructor(classOf[String], classOf[PrepareKey[_]])
      .newInstance("branch", new PrepareIntOption())

    assert(fragment.buffer.size === 0)

    val dm = new TestModel()
    for (i <- 0 until 10) {
      dm.i.modify(i)
      fragment.add(dm)
    }
    assert(fragment.buffer.size === 10)
    fragment.buffer.zipWithIndex.foreach {
      case (((b, k), dm), i) =>
        assert(b === "branch")
        assert(k.get === i)
        assert(dm.i.get === i)
    }
    fragment.reset()
    assert(fragment.buffer.size === 0)
  }
}

object OneToOneOutputFragmentClassBuilderSpec {

  class TestModel extends DataModel[TestModel] {

    val i: IntOption = new IntOption()

    override def reset: Unit = {
      i.setNull()
    }

    override def copyFrom(other: TestModel): Unit = {
      i.copyFrom(other.i)
    }
  }

  class PrepareIntOption extends PrepareKey[String] {

    override def shuffleKey[U](branch: String, value: DataModel[_]): U = {
      value.asInstanceOf[TestModel].i.asInstanceOf[U]
    }
  }
}
