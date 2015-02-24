package com.asakusafw.spark.compiler
package operator

import com.asakusafw.lang.compiler.api.JobflowProcessor.{ Context => JPContext }
import com.asakusafw.lang.compiler.model.graph._
import com.asakusafw.spark.compiler.spi._
import com.asakusafw.spark.tools.asm.ClassBuilder

object OperatorCompiler {

  case class Context(jpContext: JPContext)

  def compile(operator: Operator)(implicit context: Context): ClassBuilder = {
    operator match {
      case op: CoreOperator =>
        val compiler = CoreOperatorCompiler(context.jpContext.getClassLoader)(
          op.getCoreOperatorKind)
        compiler.compile(op)(compiler.Context(context.jpContext))
      case op: UserOperator =>
        val compiler = UserOperatorCompiler(context.jpContext.getClassLoader)(
          op.getAnnotation.getDeclaringClass.resolve(context.jpContext.getClassLoader))
        compiler.compile(op)(compiler.Context(context.jpContext))
      case op: MarkerOperator =>
        new OutputFragmentClassBuilder(op.getInput.getDataType.asType)
    }
  }
}
