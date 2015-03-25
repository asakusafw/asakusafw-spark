package com.asakusafw.spark.compiler
package operator

import org.objectweb.asm.Type

import com.asakusafw.lang.compiler.api.JobflowProcessor.{ Context => JPContext }
import com.asakusafw.lang.compiler.model.graph._
import com.asakusafw.spark.compiler.spi._

import resource._

sealed trait OperatorType

object OperatorType {

  case object MapType extends OperatorType
  case object CoGroupType extends OperatorType
  case object AggregationType extends OperatorType
}

trait OperatorCompiler {

  type Context = OperatorCompiler.Context

  def support(operator: Operator)(implicit context: Context): Boolean

  def operatorType: OperatorType

  def compile(operator: Operator)(implicit context: Context): Type
}

object OperatorCompiler {

  case class Context(
    flowId: String,
    jpContext: JPContext)

  private def getCompiler(
    operator: Operator)(implicit context: Context): Option[OperatorCompiler] = {
    operator match {
      case op: CoreOperator =>
        CoreOperatorCompiler(context.jpContext.getClassLoader).find(_.support(op))
      case op: UserOperator =>
        UserOperatorCompiler(context.jpContext.getClassLoader).find(_.support(op))
    }
  }

  def support(operator: Operator, operatorType: OperatorType)(implicit context: Context): Boolean = {
    operator match {
      case op: MarkerOperator => true
      case _ =>
        getCompiler(operator) match {
          case Some(compiler) => compiler.operatorType == operatorType
          case None           => false
        }
    }
  }

  def compile(operator: Operator, operatorType: OperatorType)(implicit context: Context): Type = {
    operator match {
      case op: MarkerOperator =>
        OutputFragmentClassBuilder.getOrCompile(
          context.flowId, op.getInput.getDataType.asType, context.jpContext)
      case _ =>
        getCompiler(operator) match {
          case Some(compiler) if compiler.operatorType == operatorType => compiler.compile(operator)
        }
    }
  }
}
