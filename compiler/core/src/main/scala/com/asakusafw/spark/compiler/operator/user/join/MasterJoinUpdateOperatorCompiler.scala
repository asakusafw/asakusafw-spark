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
    operatorInfo.annotationClass == classOf[MasterJoinUpdate]
  }

  override def operatorType: OperatorType = OperatorType.CoGroupType

  override def compile(operator: UserOperator)(implicit context: Context): Type = {
    assert(support(operator))

    val operatorInfo = new OperatorInfo(operator)(context.jpContext)

    assert(operatorInfo.inputs.size >= 2)
    assert(operatorInfo.outputs.size == 2)

    operatorInfo.outputDataModelTypes.foreach(outputDataModelType =>
      assert(outputDataModelType == operatorInfo.inputDataModelTypes(MasterJoinUpdate.ID_INPUT_TRANSACTION)))

    assert(operatorInfo.methodType.getArgumentTypes.toSeq ==
      Seq(operatorInfo.inputDataModelTypes(MasterJoinUpdate.ID_INPUT_MASTER),
        operatorInfo.inputDataModelTypes(MasterJoinUpdate.ID_INPUT_TRANSACTION))
        ++ operatorInfo.argumentTypes)

    val builder = new JoinOperatorFragmentClassBuilder(
      context.flowId,
      operatorInfo.implementationClassType,
      operatorInfo.outputs,
      operatorInfo.inputDataModelTypes(MasterJoinUpdate.ID_INPUT_MASTER),
      operatorInfo.inputDataModelTypes(MasterJoinUpdate.ID_INPUT_TRANSACTION),
      operatorInfo.selectionMethod) {

      override def join(mb: MethodBuilder, ctrl: LoopControl, masterVar: Var, txVar: Var): Unit = {
        import mb._
        masterVar.push().ifNull({
          getOutputField(mb, operatorInfo.outputs(MasterJoinUpdate.ID_OUTPUT_MISSED))
        }, {
          getOperatorField(mb)
            .invokeV(
              operatorInfo.methodDesc.getName,
              masterVar.push()
                +: txVar.push()
                +: operatorInfo.arguments.map { argument =>
                  ldc(argument.getValue.resolve(context.jpContext.getClassLoader))(
                    ClassTag(argument.getValue.getValueType.resolve(context.jpContext.getClassLoader)))
                }: _*)
          getOutputField(mb, operatorInfo.outputs(MasterJoinUpdate.ID_OUTPUT_UPDATED))
        }).invokeV("add", txVar.push().asType(classOf[AnyRef].asType))
      }
    }

    context.jpContext.addClass(builder)
  }
}
