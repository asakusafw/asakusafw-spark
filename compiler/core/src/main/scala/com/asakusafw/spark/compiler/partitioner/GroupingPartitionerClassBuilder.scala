package com.asakusafw.spark.compiler
package partitioner

import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable

import org.apache.spark.Partitioner
import org.objectweb.asm.Type

import com.asakusafw.spark.tools.asm._

class GroupingPartitionerClassBuilder private (
  flowId: String,
  properties: Seq[Type])
    extends ClassBuilder(
      Type.getType(s"Lcom/asakusafw/spark/runtime/partitioner/Partitioner$$${flowId}$$${GroupingPartitionerClassBuilder.nextId};"),
      classOf[Partitioner].asType) {

  override def defFields(fieldDef: FieldDef): Unit = {
    fieldDef.newField("numPartitions", Type.INT_TYPE)
  }

  override def defConstructors(ctorDef: ConstructorDef): Unit = {
    ctorDef.newInit(Seq(Type.INT_TYPE)) { mb =>
      import mb._
      thisVar.push().invokeInit(superType)
      thisVar.push().putField("numPartitions", Type.INT_TYPE, `var`(Type.INT_TYPE, thisVar.nextLocal).push())
    }
  }

  override def defMethods(methodDef: MethodDef): Unit = {
    methodDef.newMethod("numPartitions", Type.INT_TYPE, Seq.empty) { mb =>
      import mb._
      `return`(thisVar.push().getField("numPartitions", Type.INT_TYPE))
    }

    methodDef.newMethod("getPartition", Type.INT_TYPE, Seq(classOf[AnyRef].asType)) { mb =>
      import mb._
      val keyVar = `var`(classOf[AnyRef].asType, thisVar.nextLocal)
      val seqVar = keyVar.push().cast(classOf[Seq[_]].asType).store(keyVar.nextLocal)
      val hash = seqVar.push().ifNull(
        ldc(0),
        (ldc(1) /: properties.zipWithIndex) {
          case (result, (t, i)) =>
            result.multiply(ldc(31)).add(
              seqVar.push().invokeI("apply", classOf[AnyRef].asType, ldc(i)).cast(t.boxed)
                .invokeV("hashCode", Type.INT_TYPE))
        })
      val part = hash.remainder(thisVar.push().getField("numPartitions", Type.INT_TYPE))
      `return`(
        part.dup().ifLt0(
          part.add(thisVar.push().getField("numPartitions", Type.INT_TYPE)),
          part))
    }
  }
}

object GroupingPartitionerClassBuilder {

  private[this] val curId: AtomicLong = new AtomicLong(0L)

  def nextId: Long = curId.getAndIncrement

  private[this] val cache: mutable.Map[(String, Seq[Type]), GroupingPartitionerClassBuilder] =
    mutable.Map.empty

  def apply(flowId: String, properties: Seq[Type]): GroupingPartitionerClassBuilder = {
    cache.getOrElseUpdate(
      (flowId, properties),
      new GroupingPartitionerClassBuilder(flowId, properties))
  }
}
