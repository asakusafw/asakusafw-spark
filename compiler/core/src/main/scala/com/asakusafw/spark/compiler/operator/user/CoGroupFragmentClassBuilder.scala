package com.asakusafw.spark.compiler
package operator
package user

import java.util.concurrent.atomic.AtomicLong

import org.objectweb.asm.Type
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.spark.runtime.fragment.CoGroupFragment
import com.asakusafw.spark.tools.asm._

abstract class CoGroupFragmentClassBuilder(flowId: String)
  extends ClassBuilder(
    Type.getType(s"L${GeneratedClassPackageInternalName}/${flowId}/fragment/CoGroupFragment$$${CoGroupFragmentClassBuilder.nextId};"),
    classOf[CoGroupFragment].asType)

object CoGroupFragmentClassBuilder {

  private[this] val curId: AtomicLong = new AtomicLong(0L)

  def nextId: Long = curId.getAndIncrement
}
