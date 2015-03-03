package com.asakusafw.spark.compiler.operator
package user

import java.util.concurrent.atomic.AtomicLong

import org.objectweb.asm.Type
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.spark.runtime.fragment.CoGroupFragment
import com.asakusafw.spark.tools.asm._

abstract class CoGroupFragmentClassBuilder(
  flowId: String,
  signature: Option[String],
  superType: Type,
  interfaceTypes: Type*)
    extends ClassBuilder(
      Type.getType(s"L${classOf[CoGroupFragment].asType.getInternalName}$$${flowId}$$${CoGroupFragmentClassBuilder.nextId};"),
      signature,
      superType,
      interfaceTypes: _*) {

  def this(flowId: String) = this(flowId, None, classOf[CoGroupFragment].asType)
}

object CoGroupFragmentClassBuilder {

  private[this] val curId: AtomicLong = new AtomicLong(0L)

  def nextId: Long = curId.getAndIncrement
}
