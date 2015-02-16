package com.asakusafw.spark.compiler
package partitioner

import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.Partitioner
import org.objectweb.asm.Type

import com.asakusafw.spark.tools.asm._

class GroupingPartitionerClassBuilder private (
  keyType: Type,
  properties: Seq[MethodDesc])
    extends ClassBuilder(
      Type.getType(s"L${keyType.getInternalName}$$Grouping$$${GroupingPartitionerClassBuilder.nextId};"),
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
      val kVar = `var`(classOf[AnyRef].asType, thisVar.nextLocal)
      val keyVar = kVar.push().cast(keyType).store(kVar.nextLocal)
      val hash = keyVar.push().ifNull(
        ldc(0),
        (ldc(1) /: properties) {
          case (result, (methodName, methodType)) =>
            result.multiply(ldc(31))
              .add {
                val property = keyVar.push().invokeV(methodName, methodType.getReturnType)
                (if (property.isPrimitive) {
                  property.box()
                } else {
                  property
                }).invokeV("hashCode", Type.INT_TYPE)
              }
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

  def apply(
    ownerType: Type,
    properties: Seq[MethodDesc]): GroupingPartitionerClassBuilder = {
    new GroupingPartitionerClassBuilder(ownerType, properties)
  }
}
