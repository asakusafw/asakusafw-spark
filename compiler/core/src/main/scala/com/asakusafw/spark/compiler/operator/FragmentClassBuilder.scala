package com.asakusafw.spark.compiler
package operator

import java.util.concurrent.atomic.AtomicLong

import org.objectweb.asm.Type
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.spark.runtime.fragment.Fragment
import com.asakusafw.spark.tools.asm._

abstract class FragmentClassBuilder(
  flowId: String,
  signature: Option[String],
  superType: Type,
  interfaceTypes: Type*)
    extends ClassBuilder(
      Type.getType(s"L${GeneratedClassPackageInternalName}/${flowId}/fragment/Fragment$$${FragmentClassBuilder.nextId};"),
      signature,
      superType,
      interfaceTypes: _*) {

  def this(flowId: String, dataModelType: Type) =
    this(
      flowId,
      Option(FragmentClassBuilder.signature(dataModelType)),
      classOf[Fragment[_]].asType)
}

object FragmentClassBuilder {

  private[this] val curId: AtomicLong = new AtomicLong(0L)

  def nextId: Long = curId.getAndIncrement

  def signature(dataModelType: Type): String = {
    new ClassSignatureBuilder()
      .newSuperclass {
        _.newClassType(classOf[Fragment[_]].asType) {
          _.newTypeArgument(SignatureVisitor.INSTANCEOF) {
            _.newClassType(dataModelType)
          }
        }
      }
      .build()
  }
}
