package com.asakusafw.spark.compiler
package operator
package user
package join

import org.objectweb.asm.Type

import com.asakusafw.lang.compiler.model.graph.UserOperator
import com.asakusafw.spark.compiler.spi.UserOperatorCompiler
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._
import com.asakusafw.vocabulary.operator.MasterCheck

class MasterCheckOperatorCompiler extends UserOperatorCompiler {

  override def of: Class[_] = classOf[MasterCheck]

  override def compile(operator: UserOperator)(implicit context: Context): Type = {

    val operatorInfo = new OperatorInfo(operator)(context.jpContext)

    assert(operatorInfo.annotationDesc.getDeclaringClass.resolve(context.jpContext.getClassLoader) == of)
    assert(operatorInfo.inputs.size >= 2)

    operatorInfo.outputDataModelTypes.foreach(outputDataModelType =>
      assert(outputDataModelType == operatorInfo.inputDataModelTypes(MasterCheck.ID_INPUT_TRANSACTION)))

    val builder = new JoinOperatorFragmentClassBuilder(
      context.flowId,
      operatorInfo.implementationClassType,
      operatorInfo.outputs,
      operatorInfo.inputDataModelTypes(MasterCheck.ID_INPUT_MASTER),
      operatorInfo.inputDataModelTypes(MasterCheck.ID_INPUT_TRANSACTION),
      operatorInfo.selectionMethod) {

      override def join(mb: MethodBuilder, ctrl: LoopControl, masterVar: Var, txVar: Var): Unit = {
        import mb._
        masterVar.push().ifNull({
          getOutputField(mb, operatorInfo.outputs(MasterCheck.ID_OUTPUT_MISSED))
        }, {
          getOutputField(mb, operatorInfo.outputs(MasterCheck.ID_OUTPUT_FOUND))
        }).invokeV("add", txVar.push().asType(classOf[AnyRef].asType))
      }
    }

    context.jpContext.addClass(builder)
  }
}
