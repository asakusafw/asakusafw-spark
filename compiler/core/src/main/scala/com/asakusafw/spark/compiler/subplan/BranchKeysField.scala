package com.asakusafw.spark.compiler.subplan

import scala.collection.mutable

import org.objectweb.asm.Type
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.lang.compiler.model.graph.MarkerOperator
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._

trait BranchKeysField extends ClassBuilder {

  def outputMarkers: Seq[MarkerOperator]

  def defBranchKeysField(fieldDef: FieldDef): Unit = {
    fieldDef.newStaticFinalField("branchKeys", classOf[Set[_]].asType,
      new TypeSignatureBuilder()
        .newClassType(classOf[Set[_]].asType) {
          _.newTypeArgument(SignatureVisitor.INSTANCEOF, Type.LONG_TYPE)
        }
        .build())
  }

  def initBranchKeysField(mb: MethodBuilder): Unit = {
    import mb._
    putStatic(thisType, "branchKeys", classOf[Set[_]].asType, initBranchKeys(mb))
  }

  def initBranchKeys(mb: MethodBuilder): Stack = {
    import mb._
    getStatic(Predef.getClass.asType, "MODULE$", Predef.getClass.asType)
      .invokeV("longArrayOps", classOf[mutable.ArrayOps[_]].asType, {
        val arr = pushNewArray(Type.LONG_TYPE, outputMarkers.size)
        outputMarkers.sortBy(_.getOriginalSerialNumber).zipWithIndex.foreach {
          case (op, i) =>
            arr.dup().astore(ldc(i), ldc(op.getOriginalSerialNumber))
        }
        arr
      })
      .invokeI("toSet", classOf[Set[_]].asType)
  }

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
