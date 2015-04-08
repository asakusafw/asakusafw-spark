package com.asakusafw.spark.compiler
package subplan

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable

import org.objectweb.asm.Type

import com.asakusafw.lang.compiler.api.JobflowProcessor.{ Context => JPContext }
import com.asakusafw.spark.runtime.driver.BroadcastId
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._

class BroadcastIdsClassBuilder(flowId: String)
    extends ClassBuilder(
      Type.getType(s"L${GeneratedClassPackageInternalName}/${flowId}/driver/BroadcastIds;"),
      classOf[AnyRef].asType) {

  private[this] val broadcastIds: mutable.Map[Long, Int] = mutable.Map.empty

  private[this] val curId: AtomicInteger = new AtomicInteger(0)

  private[this] def field(id: Long): String = s"BROADCAST_${id}"

  def getField(sn: Long): String = field(broadcastIds.getOrElseUpdate(sn, curId.getAndIncrement))

  override def defFields(fieldDef: FieldDef): Unit = {
    broadcastIds.values.toSeq.sorted.foreach { id =>
      fieldDef.newStaticFinalField(field(id), classOf[BroadcastId].asType)
    }
  }

  override def defConstructors(ctorDef: ConstructorDef): Unit = {
    ctorDef.newStaticInit { mb =>
      import mb._
      broadcastIds.values.toSeq.sorted.foreach { id =>
        putStatic(thisType, field(id), classOf[BroadcastId].asType,
          getStatic(BroadcastId.getClass.asType, "MODULE$", BroadcastId.getClass.asType)
            .invokeV("apply", classOf[BroadcastId].asType, ldc(id)))
      }
    }
  }

  override def defMethods(methodDef: MethodDef): Unit = {
    super.defMethods(methodDef)

    methodDef.newStaticMethod("valueOf", classOf[BroadcastId].asType, Seq(Type.INT_TYPE)) { mb =>
      import mb._
      val idVar = `var`(Type.INT_TYPE, 0)

      def bs(min: Int, max: Int): Unit = {
        if (min < max) {
          val id = (max - min) / 2 + min
          idVar.push().unlessNe(ldc(id)) {
            `return`(getStatic(thisType, field(id), classOf[BroadcastId].asType))
          }
          idVar.push().unlessLessThanOrEqual(ldc(id)) {
            bs(id + 1, max)
          }
          bs(min, id)
        }
      }
      bs(0, curId.get)

      `throw`(pushNew0(classOf[AssertionError].asType))
    }
  }
}
