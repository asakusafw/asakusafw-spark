package com.asakusafw.spark.compiler.operator

import org.apache.spark.broadcast.Broadcast
import org.objectweb.asm.Type
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._

trait BroadcastsField extends ClassBuilder {

  def defBroadcastsField(fieldDef: FieldDef): Unit = {
    fieldDef.newFinalField("broadcasts", classOf[Map[Long, Broadcast[_]]].asType,
      new TypeSignatureBuilder()
        .newClassType(classOf[Map[Long, Broadcast[_]]].asType) {
          _.newTypeArgument(SignatureVisitor.INSTANCEOF, Type.LONG_TYPE)
            .newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[Broadcast[_]].asType)
        }
        .build())
  }

  def initBroadcastsField(mb: MethodBuilder, broadcastsVar: Var): Unit = {
    import mb._
    thisVar.push().putField("broadcasts", classOf[Map[Long, Broadcast[_]]].asType, broadcastsVar.push())
  }

  def getBroadcast(mb: MethodBuilder, sn: Long): Stack = {
    import mb._
    thisVar.push().getField("broadcasts", classOf[Map[Long, Broadcast[_]]].asType)
      .invokeI("apply", classOf[AnyRef].asType, ldc(sn).box().asType(classOf[AnyRef].asType))
      .cast(classOf[Broadcast[_]].asType)
  }
}
