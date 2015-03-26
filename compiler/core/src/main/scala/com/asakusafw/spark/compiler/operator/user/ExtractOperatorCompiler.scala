package com.asakusafw.spark.compiler
package operator
package user

import scala.reflect.ClassTag

import org.objectweb.asm.Type

import com.asakusafw.lang.compiler.model.graph.UserOperator
import com.asakusafw.runtime.core.Result
import com.asakusafw.spark.compiler.spi.OperatorType
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._
import com.asakusafw.vocabulary.operator.Extract

class ExtractOperatorCompiler extends UserOperatorCompiler {

  override def support(operator: UserOperator)(implicit context: Context): Boolean = {
    val operatorInfo = new OperatorInfo(operator)(context.jpContext)
    import operatorInfo._
    annotationDesc.resolveClass == classOf[Extract]
  }

  override def operatorType: OperatorType = OperatorType.MapType

  override def compile(operator: UserOperator)(implicit context: Context): Type = {
    assert(support(operator))

    val operatorInfo = new OperatorInfo(operator)(context.jpContext)
    import operatorInfo._

    assert(inputs.size == 1) // FIXME to take multiple inputs for side data?
    assert(outputs.size > 0)

    assert(methodDesc.parameterTypes ==
      inputs(Extract.ID_INPUT).dataModelType
      +: outputs.map(_ => classOf[Result[_]].asType)
      ++: arguments.map(_.asType))

    val builder = new UserOperatorFragmentClassBuilder(
      context.flowId,
      inputs(Extract.ID_INPUT).dataModelType,
      implementationClassType,
      outputs) {

      override def defAddMethod(mb: MethodBuilder, dataModelVar: Var): Unit = {
        import mb._
        getOperatorField(mb)
          .invokeV(
            methodDesc.getName,
            dataModelVar.push()
              +: outputs.map { output =>
                getOutputField(mb, output).asType(classOf[Result[_]].asType)
              }
              ++: arguments.map { argument =>
                ldc(argument.value)(ClassTag(argument.resolveClass))
              }: _*)
        `return`()
      }
    }

    context.jpContext.addClass(builder)
  }
}
