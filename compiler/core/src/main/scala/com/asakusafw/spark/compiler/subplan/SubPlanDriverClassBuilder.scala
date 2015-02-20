package com.asakusafw.spark.compiler.subplan

import java.util.concurrent.atomic.AtomicLong

import org.objectweb.asm._
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.spark.runtime.driver.CoGroupDriver
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._

abstract class SubPlanDriverClassBuilder(
  thisType: Type,
  signature: Option[String],
  superType: Type,
  interfaceTypes: Type*)
    extends ClassBuilder(thisType, signature, superType, interfaceTypes: _*) {

  def branchKeyType: Type
}
