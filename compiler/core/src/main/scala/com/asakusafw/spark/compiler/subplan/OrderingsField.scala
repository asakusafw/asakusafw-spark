package com.asakusafw.spark.compiler.subplan

import org.apache.spark.Partitioner
import org.objectweb.asm.Type
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.lang.compiler.api.JobflowProcessor.{ Context => JPContext }
import com.asakusafw.lang.compiler.model.graph.MarkerOperator
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._

trait OrderingsField extends ClassBuilder {

  def flowId: String

  def jpContext: JPContext

  def outputMarkers: Seq[MarkerOperator]

  def defOrderingsField(fieldDef: FieldDef): Unit = {
    fieldDef.newFinalField("orderings", classOf[Map[_, _]].asType,
      new TypeSignatureBuilder()
        .newClassType(classOf[Map[_, _]].asType) {
          _.newTypeArgument(SignatureVisitor.INSTANCEOF, Type.LONG_TYPE.boxed)
            .newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[Ordering[_]].asType)
        }
        .build())
  }

  def initOrderingsField(mb: MethodBuilder): Unit = {
    import mb._
    thisVar.push().putField("orderings", classOf[Map[_, _]].asType, initOrderings(mb))
  }

  def initOrderings(mb: MethodBuilder): Stack = {
    import mb._
    // TODO
    getStatic(Map.getClass.asType, "MODULE$", Map.getClass.asType)
      .invokeV("empty", classOf[Map[_, _]].asType)
  }

  def getOrderingsField(mb: MethodBuilder): Stack = {
    import mb._
    thisVar.push().invokeV("orderings", classOf[Map[_, _]].asType)
  }

  def defOrderings(methodDef: MethodDef): Unit = {
    methodDef.newMethod("orderings", classOf[Map[_, _]].asType, Seq.empty,
      new MethodSignatureBuilder()
        .newFormalTypeParameter("K", classOf[AnyRef].asType)
        .newReturnType {
          _.newClassType(classOf[Map[_, _]].asType) {
            _.newTypeArgument(SignatureVisitor.INSTANCEOF, Type.LONG_TYPE.boxed)
              .newTypeArgument(SignatureVisitor.INSTANCEOF) {
                _.newClassType(classOf[Ordering[_]].asType) {
                  _.newTypeArgument(SignatureVisitor.INSTANCEOF) {
                    _.newTypeVariable("K")
                  }
                }
              }
          }
        }
        .build()) { mb =>
        import mb._
        `return`(thisVar.push().getField("orderings", classOf[Map[_, _]].asType))
      }
  }
}
