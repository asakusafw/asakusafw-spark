package com.asakusafw.spark.compiler.operator.core

import org.objectweb.asm.Type

import com.asakusafw.lang.compiler.model.graph.{ CoreOperator, Operator }
import com.asakusafw.spark.compiler.spi.OperatorCompiler

trait CoreOperatorCompiler extends OperatorCompiler {

  override def support(operator: Operator)(implicit context: Context): Boolean = {
    operator match {
      case op: CoreOperator => support(op)
      case _                => false
    }
  }

  def support(operator: CoreOperator)(implicit context: Context): Boolean

  override def compile(operator: Operator)(implicit context: Context): Type = {
    operator match {
      case op: CoreOperator => compile(op)
    }
  }

  def compile(operator: CoreOperator)(implicit context: Context): Type
}
