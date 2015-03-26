package com.asakusafw.spark.compiler
package operator
package user

import scala.reflect.ClassTag

import org.objectweb.asm.Type

import com.asakusafw.lang.compiler.model.graph.UserOperator
import com.asakusafw.spark.compiler.spi.OperatorType
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._
import com.asakusafw.vocabulary.operator.Convert

class ConvertOperatorCompiler extends UserOperatorCompiler {

  override def support(operator: UserOperator)(implicit context: Context): Boolean = {
    val operatorInfo = new OperatorInfo(operator)(context.jpContext)
    import operatorInfo._
    annotationDesc.resolveClass == classOf[Convert]
  }

  override def operatorType: OperatorType = OperatorType.MapType

  override def compile(operator: UserOperator)(implicit context: Context): Type = {
    assert(support(operator))

    val operatorInfo = new OperatorInfo(operator)(context.jpContext)
    import operatorInfo._

    assert(inputs.size == 1) // FIXME to take multiple inputs for side data?
    assert(outputs.size == 2)

    assert(methodDesc.parameterTypes ==
      inputs(Convert.ID_INPUT).dataModelType
      +: arguments.map(_.asType))

    val builder = new UserOperatorFragmentClassBuilder(
      context.flowId,
      inputs(Convert.ID_INPUT).dataModelType,
      implementationClassType,
      outputs) {

      override def defAddMethod(mb: MethodBuilder, dataModelVar: Var): Unit = {
        import mb._
        getOutputField(mb, outputs(Convert.ID_OUTPUT_ORIGINAL))
          .invokeV("add", dataModelVar.push().asType(classOf[AnyRef].asType))
        getOutputField(mb, outputs(Convert.ID_OUTPUT_CONVERTED))
          .invokeV("add",
            getOperatorField(mb)
              .invokeV(
                methodDesc.getName,
                outputs(Convert.ID_OUTPUT_CONVERTED).dataModelType,
                dataModelVar.push()
                  +: arguments.map { argument =>
                    ldc(argument.value)(ClassTag(argument.resolveClass))
                  }: _*)
              .asType(classOf[AnyRef].asType))
        `return`()
      }
    }

    context.jpContext.addClass(builder)
  }
}
