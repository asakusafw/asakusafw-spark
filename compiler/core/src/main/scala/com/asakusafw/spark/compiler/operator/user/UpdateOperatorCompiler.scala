package com.asakusafw.spark.compiler
package operator
package user

import scala.reflect.ClassTag

import org.objectweb.asm.Type

import com.asakusafw.lang.compiler.model.graph.UserOperator
import com.asakusafw.spark.compiler.spi.OperatorType
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._
import com.asakusafw.vocabulary.operator.Update

class UpdateOperatorCompiler extends UserOperatorCompiler {

  override def support(operator: UserOperator)(implicit context: Context): Boolean = {
    val operatorInfo = new OperatorInfo(operator)(context.jpContext)
    import operatorInfo._
    annotationDesc.resolveClass == classOf[Update]
  }

  override def operatorType: OperatorType = OperatorType.MapType

  override def compile(operator: UserOperator)(implicit context: Context): Type = {
    assert(support(operator))

    val operatorInfo = new OperatorInfo(operator)(context.jpContext)
    import operatorInfo._

    assert(inputs.size == 1) // FIXME to take multiple inputs for side data?
    assert(outputs.size == 1)

    assert(inputs(Update.ID_INPUT).dataModelType == outputs(Update.ID_OUTPUT).dataModelType)

    methodDesc.parameterClasses
      .zip(inputs(Update.ID_INPUT).dataModelClass +: arguments.map(_.resolveClass))
      .foreach {
        case (method, model) => assert(method.isAssignableFrom(model))
      }

    val builder = new UserOperatorFragmentClassBuilder(
      context.flowId,
      inputs(Update.ID_INPUT).dataModelType,
      implementationClassType,
      outputs) {

      override def defAddMethod(mb: MethodBuilder, dataModelVar: Var): Unit = {
        import mb._
        getOperatorField(mb)
          .invokeV(
            methodDesc.getName,
            dataModelVar.push().asType(methodDesc.asType.getArgumentTypes()(0))
              +: arguments.map { argument =>
                ldc(argument.value)(ClassTag(argument.resolveClass))
              }: _*)

        getOutputField(mb, outputs.head)
          .invokeV("add", dataModelVar.push().asType(classOf[AnyRef].asType))

        `return`()
      }
    }

    context.jpContext.addClass(builder)
  }
}
