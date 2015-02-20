package com.asakusafw.spark.compiler.subplan

import org.objectweb.asm.Type
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._

trait BranchKeysField extends SubPlanDriverClassBuilder {

  def defBranchKeysField(fieldDef: FieldDef): Unit = {
    fieldDef.newStaticFinalField("branchKeys", classOf[Set[_]].asType,
      new TypeSignatureBuilder()
        .newClassType(classOf[Set[_]].asType) {
          _.newTypeArgument(SignatureVisitor.INSTANCEOF, branchKeyType)
        }
        .build())
  }

  def initBranchKeysField(mb: MethodBuilder): Unit = {
    import mb._
    putStatic(thisType, "branchKeys", classOf[Set[_]].asType, initBranchKeys(mb))
  }

  def initBranchKeys(mb: MethodBuilder): Stack

  def getBranchKeysField(mb: MethodBuilder): Stack = {
    import mb._
    thisVar.push().invokeV("branchKeys", classOf[Set[_]].asType)
  }

  def defBranchKeys(methodDef: MethodDef): Unit = {
    methodDef.newMethod("branchKeys", classOf[Set[_]].asType, Seq.empty) { mb =>
      import mb._
      `return`(getStatic(thisType, "branchKeys", classOf[Set[_]].asType))
    }
  }
}
