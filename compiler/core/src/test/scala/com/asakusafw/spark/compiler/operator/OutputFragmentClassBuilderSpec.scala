package com.asakusafw.spark.compiler.operator

import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner

import com.asakusafw.runtime.model.DataModel
import com.asakusafw.runtime.value._
import com.asakusafw.spark.runtime.fragment.OutputFragment
import com.asakusafw.spark.tools.asm._

@RunWith(classOf[JUnitRunner])
class OutputFragmentClassBuilderSpecTest extends OutputFragmentClassBuilderSpec

class OutputFragmentClassBuilderSpec extends FlatSpec with LoadClassSugar {

  import OutputFragmentClassBuilderSpec._

  behavior of classOf[OutputFragmentClassBuilder].getSimpleName

  it should "compile OutputFragment" in {
    val builder = new OutputFragmentClassBuilder(classOf[TestModel].asType)
    val cls = loadClass(builder.thisType.getClassName, builder.build()).asSubclass(classOf[OutputFragment[TestModel]])

    val fragment = cls.newInstance
    assert(fragment.buffer.size === 0)

    val dm = new TestModel()
    for (i <- 0 until 10) {
      dm.i.modify(i)
      fragment.add(dm)
    }
    assert(fragment.buffer.size === 10)
    fragment.buffer.zipWithIndex.foreach {
      case (dm, i) =>
        assert(dm.i.get === i)
    }
    fragment.reset()
    assert(fragment.buffer.size === 0)
  }
}

object OutputFragmentClassBuilderSpec {

  class TestModel extends DataModel[TestModel] {

    val i: IntOption = new IntOption()

    override def reset: Unit = {
      i.setNull()
    }

    override def copyFrom(other: TestModel): Unit = {
      i.copyFrom(other.i)
    }
  }
}
