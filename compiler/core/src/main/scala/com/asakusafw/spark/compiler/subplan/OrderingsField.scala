package com.asakusafw.spark.compiler.subplan

import org.apache.spark.Partitioner
import org.objectweb.asm.Type
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._

trait OrderingsField extends SubPlanDriverClassBuilder {

  def defOrderingsField(fieldDef: FieldDef): Unit = {
    fieldDef.newStaticFinalField("orderings", classOf[Map[_, _]].asType,
      new TypeSignatureBuilder()
        .newClassType(classOf[Map[_, _]].asType) {
          _.newTypeArgument(SignatureVisitor.INSTANCEOF, branchKeyType)
            .newTypeArgument(SignatureVisitor.INSTANCEOF) {
              _.newClassType(classOf[Ordering[_]].asType) {
                _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[AnyRef].asType)
              }
            }
        }
        .build())
  }

  def initOrderingsField(mb: MethodBuilder): Unit = {
    import mb._
    putStatic(thisType, "orderings", classOf[Map[_, _]].asType, initOrderings(mb))
  }

  def initOrderings(mb: MethodBuilder): Stack

  def getOrderingsField(mb: MethodBuilder): Stack = {
    import mb._
    thisVar.push().invokeV("orderings", classOf[Map[_, _]].asType)
  }

  def defOrderings(methodDef: MethodDef): Unit = {
    methodDef.newMethod("orderings", classOf[Map[_, _]].asType, Seq.empty,
      new MethodSignatureBuilder()
        .newReturnType {
          _.newClassType(classOf[Map[_, _]].asType) {
            _.newTypeArgument(SignatureVisitor.INSTANCEOF, branchKeyType)
              .newTypeArgument(SignatureVisitor.INSTANCEOF) {
                _.newClassType(classOf[Ordering[_]].asType) {
                  _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[AnyRef].asType)
                }
              }
          }
        }
        .build()) { mb =>
        import mb._
        `return`(getStatic(thisType, "orderings", classOf[Map[_, _]].asType))
      }
  }
}
