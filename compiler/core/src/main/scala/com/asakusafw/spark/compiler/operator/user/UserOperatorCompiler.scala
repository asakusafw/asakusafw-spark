package com.asakusafw.spark.compiler.operator.user

import org.objectweb.asm.Type

import com.asakusafw.lang.compiler.model.graph.{ Operator, UserOperator }
import com.asakusafw.spark.compiler.spi.OperatorCompiler

trait UserOperatorCompiler extends OperatorCompiler {

  override def support(operator: Operator)(implicit context: Context): Boolean = {
    operator match {
      case op: UserOperator => support(op)
      case _                => false
    }
  }

  def support(operator: UserOperator)(implicit context: Context): Boolean

  override def compile(operator: Operator)(implicit context: Context): Type = {
    operator match {
      case op: UserOperator => compile(op)
    }
  }

  def compile(operator: UserOperator)(implicit context: Context): Type
}
