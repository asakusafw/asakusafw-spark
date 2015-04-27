package com.asakusafw.spark.compiler.operator

import org.apache.spark.broadcast.Broadcast
import org.objectweb.asm.Type
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.spark.runtime.driver.BroadcastId
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._

trait BroadcastsField extends ClassBuilder {

  def defBroadcastsField(fieldDef: FieldDef): Unit = {
    fieldDef.newFinalField("broadcasts", classOf[Map[BroadcastId, Broadcast[_]]].asType,
      new TypeSignatureBuilder()
        .newClassType(classOf[Map[BroadcastId, Broadcast[_]]].asType) {
          _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[BroadcastId].asType)
            .newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[Broadcast[_]].asType)
        }
        .build())
  }

  def initBroadcastsField(mb: MethodBuilder, broadcastsVar: Var): Unit = {
    import mb._
    thisVar.push().putField("broadcasts", classOf[Map[BroadcastId, Broadcast[_]]].asType, broadcastsVar.push())
  }

  def getBroadcastsField(mb: MethodBuilder): Stack = {
    import mb._
    thisVar.push().getField("broadcasts", classOf[Map[BroadcastId, Broadcast[_]]].asType)
  }
}
