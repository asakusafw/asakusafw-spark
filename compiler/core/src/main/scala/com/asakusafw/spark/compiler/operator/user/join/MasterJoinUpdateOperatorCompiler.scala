package com.asakusafw.spark.compiler
package operator
package user
package join

import scala.reflect.ClassTag

import org.objectweb.asm.Type

import com.asakusafw.lang.compiler.model.graph.UserOperator
import com.asakusafw.spark.compiler.spi.OperatorType
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._
import com.asakusafw.vocabulary.operator.MasterJoinUpdate

class MasterJoinUpdateOperatorCompiler extends UserOperatorCompiler {

  override def support(operator: UserOperator)(implicit context: Context): Boolean = {
    val operatorInfo = new OperatorInfo(operator)(context.jpContext)
    import operatorInfo._
    annotationDesc.resolveClass == classOf[MasterJoinUpdate]
  }

  override def operatorType: OperatorType = OperatorType.CoGroupType

  override def compile(operator: UserOperator)(implicit context: Context): Type = {
    assert(support(operator))

    val operatorInfo = new OperatorInfo(operator)(context.jpContext)
    import operatorInfo._

    assert(inputs.size >= 2)
    assert(outputs.size == 2)

    outputs.foreach { output =>
      assert(output.dataModelType == inputs(MasterJoinUpdate.ID_INPUT_TRANSACTION).dataModelType)
    }

    methodDesc.parameterClasses
      .zip(inputs(MasterJoinUpdate.ID_INPUT_MASTER).dataModelClass
        +: inputs(MasterJoinUpdate.ID_INPUT_TRANSACTION).dataModelClass
        +: arguments.map(_.resolveClass))
      .foreach {
        case (method, model) => assert(method.isAssignableFrom(model))
      }

    val builder = new JoinOperatorFragmentClassBuilder(
      context.flowId,
      implementationClassType,
      outputs,
      inputs(MasterJoinUpdate.ID_INPUT_MASTER).dataModelType,
      inputs(MasterJoinUpdate.ID_INPUT_TRANSACTION).dataModelType,
      selectionMethod) {

      override def join(mb: MethodBuilder, ctrl: LoopControl, masterVar: Var, txVar: Var): Unit = {
        import mb._
        masterVar.push().ifNull({
          getOutputField(mb, outputs(MasterJoinUpdate.ID_OUTPUT_MISSED))
        }, {
          getOperatorField(mb)
            .invokeV(
              methodDesc.getName,
              masterVar.push().asType(methodDesc.asType.getArgumentTypes()(0))
                +: txVar.push().asType(methodDesc.asType.getArgumentTypes()(1))
                +: arguments.map { argument =>
                  ldc(argument.value)(ClassTag(argument.resolveClass))
                }: _*)
          getOutputField(mb, outputs(MasterJoinUpdate.ID_OUTPUT_UPDATED))
        }).invokeV("add", txVar.push().asType(classOf[AnyRef].asType))
      }
    }

    context.jpContext.addClass(builder)
  }
}
