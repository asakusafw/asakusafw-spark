package com.asakusafw.spark.compiler.subplan

import org.apache.spark.Partitioner
import org.objectweb.asm.Type
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.lang.compiler.api.JobflowProcessor.{ Context => JPContext }
import com.asakusafw.lang.compiler.model.graph.MarkerOperator
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._

trait PartitionersField extends ClassBuilder {

  def flowId: String

  def jpContext: JPContext

  def outputMarkers: Seq[MarkerOperator]

  def defPartitionersField(fieldDef: FieldDef): Unit = {
    fieldDef.newStaticFinalField("partitioners", classOf[Map[_, _]].asType,
      new TypeSignatureBuilder()
        .newClassType(classOf[Map[_, _]].asType) {
          _.newTypeArgument(SignatureVisitor.INSTANCEOF, Type.LONG_TYPE.boxed)
            .newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[Partitioner].asType)
        }
        .build())
  }

  def initPartitionersField(mb: MethodBuilder): Unit = {
    import mb._
    putStatic(thisType, "partitioners", classOf[Map[_, _]].asType, initPartitioners(mb))
  }

  def initPartitioners(mb: MethodBuilder): Stack = {
    import mb._
    // TODO
    getStatic(Map.getClass.asType, "MODULE$", Map.getClass.asType)
      .invokeV("empty", classOf[Map[_, _]].asType)
  }

  def getPartitionersField(mb: MethodBuilder): Stack = {
    import mb._
    thisVar.push().invokeV("partitioners", classOf[Map[_, _]].asType)
  }

  def defPartitioners(methodDef: MethodDef): Unit = {
    methodDef.newMethod("partitioners", classOf[Map[_, _]].asType, Seq.empty,
      new MethodSignatureBuilder()
        .newReturnType {
          _.newClassType(classOf[Map[_, _]].asType) {
            _.newTypeArgument(SignatureVisitor.INSTANCEOF, Type.LONG_TYPE.boxed)
              .newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[Partitioner].asType)
          }
        }
        build ()) { mb =>
        import mb._
        `return`(getStatic(thisType, "partitioners", classOf[Map[_, _]].asType))
      }
  }
}
