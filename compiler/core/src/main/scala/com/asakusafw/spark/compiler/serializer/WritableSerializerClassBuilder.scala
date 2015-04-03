package com.asakusafw.spark.compiler
package serializer

import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable

import org.apache.hadoop.io.Writable
import org.objectweb.asm.Type
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.lang.compiler.api.JobflowProcessor.{ Context => JPContext }
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.spark.runtime.serializer.WritableSerializer
import com.asakusafw.spark.tools.asm._

class WritableSerializerClassBuilder(flowId: String, writableType: Type)
    extends ClassBuilder(
      Type.getType(s"L${GeneratedClassPackageInternalName}/${flowId}/serializer/WritableSerializer$$${WritableSerializerClassBuilder.nextId};"),
      Some(WritableSerializerClassBuilder.signature(writableType)),
      classOf[WritableSerializer[_]].asType) {

  override def defMethods(methodDef: MethodDef): Unit = {
    super.defMethods(methodDef)

    methodDef.newMethod("newInstance", writableType, Seq.empty) { mb =>
      import mb._
      `return`(pushNew0(writableType))
    }

    methodDef.newMethod("newInstance", classOf[Writable].asType, Seq.empty) { mb =>
      import mb._
      `return`(thisVar.push().invokeV("newInstance", writableType))
    }
  }
}

object WritableSerializerClassBuilder {

  private[this] val curId: AtomicLong = new AtomicLong(0L)

  def nextId: Long = curId.getAndIncrement

  def signature(dataModelType: Type): String = {
    new ClassSignatureBuilder()
      .newSuperclass {
        _.newClassType(classOf[WritableSerializer[_]].asType) {
          _.newTypeArgument(SignatureVisitor.INSTANCEOF) {
            _.newClassType(dataModelType)
          }
        }
      }
      .build()
  }

  private[this] val cache: mutable.Map[JPContext, mutable.Map[(String, Type), Type]] =
    mutable.WeakHashMap.empty

  def getOrCompile(
    flowId: String,
    writableType: Type,
    jpContext: JPContext): Type = {
    cache.getOrElseUpdate(jpContext, mutable.Map.empty).getOrElseUpdate(
      (flowId, writableType), {
        jpContext.addClass(new WritableSerializerClassBuilder(flowId, writableType))
      })
  }
}
