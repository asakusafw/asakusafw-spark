package com.asakusafw.spark.compiler.subplan

import scala.collection.mutable
import scala.reflect.NameTransformer

import org.objectweb.asm.Type
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.lang.compiler.planning.SubPlan
import com.asakusafw.spark.runtime.driver.BranchKey
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._

trait BranchKeysField extends ClassBuilder {

  def branchKeys: BranchKeysClassBuilder

  def subplanOutputs: Seq[SubPlan.Output]

  def defBranchKeysField(fieldDef: FieldDef): Unit = {
    fieldDef.newFinalField("branchKeys", classOf[Set[_]].asType,
      new TypeSignatureBuilder()
        .newClassType(classOf[Set[_]].asType) {
          _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[BranchKey].asType)
        }
        .build())
  }

  def initBranchKeysField(mb: MethodBuilder): Unit = {
    import mb._
    thisVar.push().putField("branchKeys", classOf[Set[_]].asType, initBranchKeys(mb))
  }

  def initBranchKeys(mb: MethodBuilder): Stack = {
    import mb._
    val builder = getStatic(Set.getClass.asType, "MODULE$", Set.getClass.asType)
      .invokeV("newBuilder", classOf[mutable.Builder[_, _]].asType)

    subplanOutputs.map(_.getOperator.getOriginalSerialNumber).sorted.foreach { sn =>
      builder.invokeI(
        NameTransformer.encode("+="),
        classOf[mutable.Builder[_, _]].asType,
        getStatic(branchKeys.thisType, branchKeys.getField(sn), classOf[BranchKey].asType).asType(classOf[AnyRef].asType))
    }

    builder.invokeI("result", classOf[AnyRef].asType).cast(classOf[Set[_]].asType)
  }

  def getBranchKeysField(mb: MethodBuilder): Stack = {
    import mb._
    thisVar.push().invokeV("branchKeys", classOf[Set[_]].asType)
  }

  def defBranchKeys(methodDef: MethodDef): Unit = {
    methodDef.newMethod("branchKeys", classOf[Set[_]].asType, Seq.empty,
      new MethodSignatureBuilder()
        .newReturnType {
          _.newClassType(classOf[Set[_]].asType) {
            _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[BranchKey].asType)
          }
        }
        .build()) { mb =>
        import mb._
        `return`(thisVar.push().getField("branchKeys", classOf[Set[_]].asType))
      }
  }
}
