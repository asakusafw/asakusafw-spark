package com.asakusafw.spark.compiler.subplan

import java.util.concurrent.atomic.AtomicLong

import org.objectweb.asm._
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.spark.runtime.driver.CoGroupDriver
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._

abstract class CoGroupDriverClassBuilder(
  val branchKeyType: Type,
  val groupingKeyType: Type)
    extends SubPlanDriverClassBuilder(
      Type.getType(s"L${classOf[CoGroupDriver[_, _]].asType.getInternalName}$$${CoGroupDriverClassBuilder.nextId};"),
      Option(CoGroupDriverClassBuilder.signature(branchKeyType, groupingKeyType)),
      classOf[CoGroupDriver[_, _]].asType) {

}

object CoGroupDriverClassBuilder {

  private[this] val curId: AtomicLong = new AtomicLong(0L)

  def nextId: Long = curId.getAndIncrement

  def signature(branchKeyType: Type, groupingKeyType: Type): String = {
    new ClassSignatureBuilder()
      .newSuperclass {
        _.newClassType(classOf[CoGroupDriver[_, _]].asType) {
          _
            .newTypeArgument(SignatureVisitor.INSTANCEOF, branchKeyType)
            .newTypeArgument(SignatureVisitor.INSTANCEOF, groupingKeyType)
        }
      }
      .build()
  }
}
