package com.asakusafw.spark.compiler
package operator
package user

import scala.reflect.ClassTag

import org.objectweb.asm.Type

import com.asakusafw.lang.compiler.model.graph.UserOperator
import com.asakusafw.runtime.core.Result
import com.asakusafw.spark.compiler.spi.UserOperatorCompiler
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._
import com.asakusafw.vocabulary.operator.Extract

class ExtractOperatorCompiler extends UserOperatorCompiler {

  override def of: Class[_] = classOf[Extract]

  override def compile(operator: UserOperator)(implicit context: Context): Type = {

    val operatorInfo = new OperatorInfo(operator)(context.jpContext)

    assert(operatorInfo.annotationDesc.getDeclaringClass.resolve(context.jpContext.getClassLoader) == of)
    assert(operatorInfo.inputs.size == 1) // FIXME to take multiple inputs for side data?
    assert(operatorInfo.outputs.size > 0)

    assert(operatorInfo.methodType.getArgumentTypes.toSeq ==
      operatorInfo.inputDataModelTypes(Extract.ID_INPUT)
      +: operatorInfo.outputDataModelTypes.map(_ => classOf[Result[_]].asType)
      ++: operatorInfo.argumentTypes)

    val builder = new UserOperatorFragmentClassBuilder(
      context.flowId,
      operatorInfo.inputDataModelTypes(Extract.ID_INPUT),
      operatorInfo.implementationClassType,
      operatorInfo.outputs) {

      override def defAddMethod(mb: MethodBuilder, dataModelVar: Var): Unit = {
        import mb._
        getOperatorField(mb)
          .invokeV(
            operatorInfo.methodDesc.getName,
            dataModelVar.push()
              +: operatorInfo.outputs.map { output =>
                getOutputField(mb, output).asType(classOf[Result[_]].asType)
              }
              ++: operatorInfo.arguments.map { argument =>
                ldc(argument.getValue.resolve(context.jpContext.getClassLoader))(
                  ClassTag(argument.getValue.getValueType.resolve(context.jpContext.getClassLoader)))
              }: _*)
        `return`()
      }
    }

    context.jpContext.addClass(builder)
  }
}
