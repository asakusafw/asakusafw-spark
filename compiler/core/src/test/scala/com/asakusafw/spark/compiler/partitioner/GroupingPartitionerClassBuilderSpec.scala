package com.asakusafw.spark.compiler.partitioner

import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner

import org.apache.spark.Partitioner
import org.objectweb.asm.Type

import com.asakusafw.runtime.model.DataModel
import com.asakusafw.runtime.value._
import com.asakusafw.spark.tools.asm._

@RunWith(classOf[JUnitRunner])
class GroupingPartitionerClassBuilderSpecTest extends GroupingPartitionerClassBuilderSpec

class GroupingPartitionerClassBuilderSpec extends FlatSpec with LoadClassSugar {

  import GroupingPartitionerClassBuilderSpec._

  behavior of classOf[GroupingPartitionerClassBuilder].getSimpleName

  it should "build grouping partitioner class" in {
    val dataModelType = classOf[TestModel].asType
    val builder = GroupingPartitionerClassBuilder(
      dataModelType, Seq(
        ("i", Type.getMethodType(Type.INT_TYPE)),
        ("iOpt", Type.getMethodType(classOf[IntOption].asType))))

    val partitioner = loadClass(builder.thisType.getClassName, builder.build())
      .asSubclass(classOf[Partitioner])
      .getConstructor(classOf[Int]).newInstance(Int.box(10))
    assert(partitioner.numPartitions === 10)

    assert(partitioner.getPartition(null) === 0)

    val dm = new TestModel()
    assert(partitioner.getPartition(dm) === (((1 * 31) + 0) * 31 + 1) % 10)
    dm.i = 10
    assert(partitioner.getPartition(dm) === (((1 * 31) + 10) * 31 + 1) % 10)
    dm.iOpt.modify(100)
    assert(partitioner.getPartition(dm) === (((1 * 31) + 10) * 31 + ((1 * 31) + 100)) % 10)
    dm.j = 1000
    assert(partitioner.getPartition(dm) === (((1 * 31) + 10) * 31 + ((1 * 31) + 100)) % 10)
    dm.jOpt.modify(10000)
    assert(partitioner.getPartition(dm) === (((1 * 31) + 10) * 31 + ((1 * 31) + 100)) % 10)
  }
}

object GroupingPartitionerClassBuilderSpec {

  class TestModel extends DataModel[TestModel] {

    var i: Int = 0
    val iOpt: IntOption = new IntOption()
    var j: Long = 0
    val jOpt: LongOption = new LongOption()

    override def reset: Unit = {
      i = 0
      iOpt.setNull()
      j = 0
      jOpt.setNull()
    }

    override def copyFrom(other: TestModel): Unit = {
      i = other.i
      iOpt.copyFrom(other.iOpt)
      j = other.j
      jOpt.copyFrom(other.jOpt)
    }
  }
}
