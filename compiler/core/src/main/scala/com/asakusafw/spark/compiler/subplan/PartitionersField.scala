package com.asakusafw.spark.compiler.subplan

import org.apache.spark.Partitioner
import org.objectweb.asm.Type
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._

trait PartitionersField extends SubPlanDriverClassBuilder {

  def defPartitionersField(fieldDef: FieldDef): Unit = {
    fieldDef.newStaticFinalField("partitioners", classOf[Map[_, _]].asType,
      new TypeSignatureBuilder()
        .newClassType(classOf[Map[_, _]].asType) {
          _.newTypeArgument(SignatureVisitor.INSTANCEOF, branchKeyType)
            .newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[Partitioner].asType)
        }
        .build())
  }

  def initPartitionersField(mb: MethodBuilder): Unit = {
    import mb._
    putStatic(thisType, "partitioners", classOf[Map[_, _]].asType, initPartitioners(mb))
  }

  def initPartitioners(mb: MethodBuilder): Stack

  def getPartitionersField(mb: MethodBuilder): Stack = {
    import mb._
    thisVar.push().invokeV("partitioners", classOf[Map[_, _]].asType)
  }

  def defPartitioners(methodDef: MethodDef): Unit = {
    methodDef.newMethod("partitioners", classOf[Map[_, _]].asType, Seq.empty) { mb =>
      import mb._
      `return`(getStatic(thisType, "partitioners", classOf[Map[_, _]].asType))
    }
  }
}
