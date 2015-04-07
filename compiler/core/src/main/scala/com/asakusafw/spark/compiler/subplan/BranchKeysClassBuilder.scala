package com.asakusafw.spark.compiler
package subplan

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable

import org.objectweb.asm.Type

import com.asakusafw.lang.compiler.api.JobflowProcessor.{ Context => JPContext }
import com.asakusafw.spark.runtime.driver.BranchKey
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._

class BranchKeysClassBuilder(flowId: String)
    extends ClassBuilder(
      Type.getType(s"L${GeneratedClassPackageInternalName}/${flowId}/driver/BranchKeys;"),
      classOf[AnyRef].asType) {

  private[this] val branchKeys: mutable.Map[Long, Long] = mutable.Map.empty

  private[this] val curId: AtomicInteger = new AtomicInteger(0)

  private[this] def field(id: Long): String = s"branch${id}"

  def getField(sn: Long): String = field(branchKeys.getOrElseUpdate(sn, curId.getAndIncrement))

  override def defFields(fieldDef: FieldDef): Unit = {
    branchKeys.values.toSeq.sorted.foreach { id =>
      fieldDef.newStaticFinalField(field(id), classOf[BranchKey].asType)
    }
  }

  override def defConstructors(ctorDef: ConstructorDef): Unit = {
    ctorDef.newStaticInit { mb =>
      import mb._
      branchKeys.values.toSeq.sorted.foreach { id =>
        putStatic(thisType, field(id), classOf[BranchKey].asType,
          getStatic(BranchKey.getClass.asType, "MODULE$", BranchKey.getClass.asType)
            .invokeV("apply", classOf[BranchKey].asType, ldc(id)))
      }
    }
  }

  override def defMethods(methodDef: MethodDef): Unit = {
    super.defMethods(methodDef)

    methodDef.newStaticMethod("valueOf", classOf[BranchKey].asType, Seq(Type.LONG_TYPE)) { mb =>
      import mb._
      val idVar = `var`(Type.LONG_TYPE, 0)
      branchKeys.values.toSeq.sorted.foreach { id =>
        idVar.push().unlessNe(ldc(id)) {
          `return`(
            getStatic(thisType, field(id), classOf[BranchKey].asType))
        }
      }
      `throw`(pushNew0(classOf[AssertionError].asType))
    }
  }
}
