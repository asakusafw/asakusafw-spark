package com.asakusafw.spark.compiler.operator

import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner

import com.asakusafw.runtime.model.DataModel
import com.asakusafw.runtime.value._
import com.asakusafw.spark.runtime.driver.PrepareKey
import com.asakusafw.spark.runtime.fragment._
import com.asakusafw.spark.tools.asm._

@RunWith(classOf[JUnitRunner])
class EdgeFragmentClassBuilderSpecTest extends EdgeFragmentClassBuilderSpec

class EdgeFragmentClassBuilderSpec extends FlatSpec with LoadClassSugar {

  import EdgeFragmentClassBuilderSpec._

  behavior of classOf[EdgeFragmentClassBuilder].getSimpleName

  it should "compile EdgeFragment" in {
    val prepareKey = new PrepareIntOption()

    val out1 = {
      val builder = new OneToOneOutputFragmentClassBuilder(
        "flowId", classOf[String].asType, classOf[TestModel].asType, classOf[IntOption].asType)
      val cls = loadClass(builder.thisType.getClassName, builder.build())
        .asSubclass(classOf[OneToOneOutputFragment[String, TestModel, IntOption]])
      cls.getConstructor(classOf[String], classOf[PrepareKey[_]]).newInstance("out1", prepareKey)
    }

    val out2 = {
      val builder = new OneToOneOutputFragmentClassBuilder(
        "flowId", classOf[String].asType, classOf[TestModel].asType, classOf[IntOption].asType)
      val cls = loadClass(builder.thisType.getClassName, builder.build())
        .asSubclass(classOf[OneToOneOutputFragment[String, TestModel, IntOption]])
      cls.getConstructor(classOf[String], classOf[PrepareKey[_]]).newInstance("out2", prepareKey)
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
    assert(out1.buffer.size === 10)
    assert(out2.buffer.size === 10)
    out1.buffer.zipWithIndex.foreach {
      case (((b, k), dm), i) =>
        assert(b === "out1")
        assert(k.get === i)
        assert(dm.i.get === i)
    }
    out2.buffer.zipWithIndex.foreach {
      case (((b, k), dm), i) =>
        assert(b === "out2")
        assert(k.get === i)
        assert(dm.i.get === i)
    }
    fragment.reset()
    assert(out1.buffer.size === 0)
    assert(out2.buffer.size === 0)
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

  class PrepareIntOption extends PrepareKey[String] {

    override def shuffleKey[U](branch: String, value: DataModel[_]): U = {
      value.asInstanceOf[TestModel].i.asInstanceOf[U]
    }
  }
}
