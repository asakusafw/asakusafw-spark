package com.asakusafw.spark.compiler
package operator

import org.objectweb.asm.Type

import com.asakusafw.lang.compiler.api.JobflowProcessor.{ Context => JPContext }
import com.asakusafw.lang.compiler.model.description.ClassDescription
import com.asakusafw.lang.compiler.model.graph._
import com.asakusafw.spark.compiler.spi._
import com.asakusafw.spark.tools.asm.ClassBuilder

import resource._

trait OperatorCompiler {

  type Context = OperatorCompiler.Context
}

object OperatorCompiler {

  case class Context(flowId: String, jpContext: JPContext)

  def compile(operator: Operator)(implicit context: Context): Type = {
    operator match {
      case op: CoreOperator =>
        CoreOperatorCompiler(context.jpContext.getClassLoader)(
          op.getCoreOperatorKind).compile(op)
      case op: UserOperator =>
        UserOperatorCompiler(context.jpContext.getClassLoader)(
          op.getAnnotation.getDeclaringClass.resolve(context.jpContext.getClassLoader))
          .compile(op)
      case op: MarkerOperator =>
        OutputFragmentClassBuilder.getOrCompile(
          context.flowId, op.getInput.getDataType.asType, context.jpContext)
    }
  }
}
