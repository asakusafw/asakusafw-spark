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

  behavior of classOf[GroupingPartitionerClassBuilder].getSimpleName

  it should "build grouping partitioner class" in {
    val builder = GroupingPartitionerClassBuilder(
      "flowId", Seq(Type.INT_TYPE, classOf[IntOption].asType))

    val partitioner = loadClass(builder.thisType.getClassName, builder.build())
      .asSubclass(classOf[Partitioner])
      .getConstructor(classOf[Int]).newInstance(Int.box(10))
    assert(partitioner.numPartitions === 10)

    assert(partitioner.getPartition(null) === 0)

    assert(partitioner.getPartition(Seq[Any](0, new IntOption(), 0L, new LongOption()))
      === (((1 * 31) + 0) * 31 + 1) % 10)
    assert(partitioner.getPartition(Seq[Any](10, new IntOption(), 0L, new LongOption()))
      === (((1 * 31) + 10) * 31 + 1) % 10)
    assert(partitioner.getPartition(Seq[Any](10, new IntOption().modify(100), 0L, new LongOption()))
      === (((1 * 31) + 10) * 31 + ((1 * 31) + 100)) % 10)
    assert(partitioner.getPartition(Seq[Any](10, new IntOption().modify(100), 1000L, new LongOption()))
      === (((1 * 31) + 10) * 31 + ((1 * 31) + 100)) % 10)
    assert(partitioner.getPartition(Seq[Any](10, new IntOption().modify(100), 1000L, new LongOption().modify(10000L)))
      === (((1 * 31) + 10) * 31 + ((1 * 31) + 100)) % 10)
  }
}
