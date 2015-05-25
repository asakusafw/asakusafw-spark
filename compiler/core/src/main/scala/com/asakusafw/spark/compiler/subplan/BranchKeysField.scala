package com.asakusafw.spark.compiler.subplan

import scala.collection.mutable
import scala.reflect.NameTransformer

import org.objectweb.asm.{ Opcodes, Type }
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.lang.compiler.planning.SubPlan
import com.asakusafw.spark.runtime.rdd.BranchKey
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._

trait BranchKeysField extends ClassBuilder {

  def branchKeys: BranchKeys

  def subplanOutputs: Seq[SubPlan.Output]

  override def defFields(fieldDef: FieldDef): Unit = {
    super.defFields(fieldDef)

    fieldDef.newField(
      Opcodes.ACC_PRIVATE | Opcodes.ACC_TRANSIENT,
      "branchKeys",
      classOf[Set[_]].asType,
      new TypeSignatureBuilder()
        .newClassType(classOf[Set[_]].asType) {
          _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[BranchKey].asType)
        }
        .build())
  }

  override def defMethods(methodDef: MethodDef): Unit = {
    super.defMethods(methodDef)

    methodDef.newMethod("branchKeys", classOf[Set[_]].asType, Seq.empty,
      new MethodSignatureBuilder()
        .newReturnType {
          _.newClassType(classOf[Set[_]].asType) {
            _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[BranchKey].asType)
          }
        }
        .build()) { mb =>
        import mb._
        thisVar.push().getField("branchKeys", classOf[Set[_]].asType).unlessNotNull {
          thisVar.push().putField("branchKeys", classOf[Set[_]].asType, initBranchKeys(mb))
        }
        `return`(thisVar.push().getField("branchKeys", classOf[Set[_]].asType))
      }
  }

  def getBranchKeysField(mb: MethodBuilder): Stack = {
    import mb._
    thisVar.push().invokeV("branchKeys", classOf[Set[_]].asType)
  }

  private def initBranchKeys(mb: MethodBuilder): Stack = {
    import mb._
    val builder = getStatic(Set.getClass.asType, "MODULE$", Set.getClass.asType)
      .invokeV("newBuilder", classOf[mutable.Builder[_, _]].asType)

    subplanOutputs.map(_.getOperator).sortBy(_.getSerialNumber).foreach { marker =>
      builder.invokeI(
        NameTransformer.encode("+="),
        classOf[mutable.Builder[_, _]].asType,
        branchKeys.getField(mb, marker).asType(classOf[AnyRef].asType))
    }

    builder.invokeI("result", classOf[AnyRef].asType).cast(classOf[Set[_]].asType)
  }
}
