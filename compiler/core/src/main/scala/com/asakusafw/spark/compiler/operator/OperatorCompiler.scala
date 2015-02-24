package com.asakusafw.spark.compiler
package operator

import org.objectweb.asm.Type

import com.asakusafw.lang.compiler.api.JobflowProcessor.{ Context => JPContext }
import com.asakusafw.lang.compiler.model.graph._
import com.asakusafw.spark.compiler.spi._
import com.asakusafw.spark.tools.asm.ClassBuilder

trait OperatorCompiler {

  type Context = OperatorCompiler.Context
}

object OperatorCompiler {

  case class Context(jpContext: JPContext)

  def compile(operator: Operator)(implicit context: Context): (Type, Array[Byte]) = {
    operator match {
      case op: CoreOperator =>
        CoreOperatorCompiler(context.jpContext.getClassLoader)(
          op.getCoreOperatorKind).compile(op)
      case op: UserOperator =>
        UserOperatorCompiler(context.jpContext.getClassLoader)(
          op.getAnnotation.getDeclaringClass.resolve(context.jpContext.getClassLoader))
          .compile(op)
      case op: MarkerOperator =>
        val builder = new OutputFragmentClassBuilder(op.getInput.getDataType.asType)
        (builder.thisType, builder.build())
    }
  }
}
