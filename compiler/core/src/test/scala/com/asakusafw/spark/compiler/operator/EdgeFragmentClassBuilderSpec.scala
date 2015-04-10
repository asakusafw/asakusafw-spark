package com.asakusafw.spark.compiler.operator

import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner

import com.asakusafw.runtime.model.DataModel
import com.asakusafw.runtime.value._
import com.asakusafw.spark.runtime.fragment._
import com.asakusafw.spark.tools.asm._

@RunWith(classOf[JUnitRunner])
class EdgeFragmentClassBuilderSpecTest extends EdgeFragmentClassBuilderSpec

class EdgeFragmentClassBuilderSpec extends FlatSpec with LoadClassSugar {

  import EdgeFragmentClassBuilderSpec._

  behavior of classOf[EdgeFragmentClassBuilder].getSimpleName

  it should "compile EdgeFragment" in {
    val (out1, out2) = {
      val builder = new OutputFragmentClassBuilder("flowId", classOf[TestModel].asType)
      val cls = loadClass(builder.thisType.getClassName, builder.build())
        .asSubclass(classOf[OutputFragment[TestModel]])
      (cls.newInstance(), cls.newInstance())
    }

    val builder = new EdgeFragmentClassBuilder("flowId", classOf[TestModel].asType)
    val cls = loadClass(builder.thisType.getClassName, builder.build())
      .asSubclass(classOf[EdgeFragment[TestModel]])

    val fragment = cls.getConstructor(classOf[Seq[Fragment[_]]]).newInstance(Seq(out1, out2))

    val dm = new TestModel()
    for (i <- 0 until 10) {
      dm.i.modify(i)
      fragment.add(dm)
    }
    assert(out1.size === 10)
    assert(out2.size === 10)
    out1.zipWithIndex.foreach {
      case (dm, i) =>
        assert(dm.i.get === i)
    }
    out2.zipWithIndex.foreach {
      case (dm, i) =>
        assert(dm.i.get === i)
    }
    fragment.reset()
    assert(out1.size === 0)
    assert(out2.size === 0)
  }
}

object EdgeFragmentClassBuilderSpec {

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
