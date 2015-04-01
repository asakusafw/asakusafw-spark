package com.asakusafw.spark.compiler
package operator
package user
package join

import org.objectweb.asm.Type

import com.asakusafw.lang.compiler.model.graph.UserOperator
import com.asakusafw.spark.compiler.spi.OperatorType
import com.asakusafw.vocabulary.operator.{ MasterCheck => MasterCheckOp }

class ShuffledMasterCheckOperatorCompiler extends UserOperatorCompiler {

  override def support(operator: UserOperator)(implicit context: Context): Boolean = {
    val operatorInfo = new OperatorInfo(operator)(context.jpContext)
    import operatorInfo._
    annotationDesc.resolveClass == classOf[MasterCheckOp]
  }

  override def operatorType: OperatorType = OperatorType.CoGroupType

  override def compile(operator: UserOperator)(implicit context: Context): Type = {
    assert(support(operator))

    val operatorInfo = new OperatorInfo(operator)(context.jpContext)
    import operatorInfo._

    assert(inputs.size >= 2)

    outputs.foreach(output =>
      assert(output.dataModelType == inputs(MasterCheckOp.ID_INPUT_TRANSACTION).dataModelType))

    val builder = new JoinOperatorFragmentClassBuilder(
      context.flowId,
      implementationClassType,
      outputs) with ShuffledJoin with MasterCheck {

      val masterType: Type = inputs(MasterCheckOp.ID_INPUT_MASTER).dataModelType
      val txType: Type = inputs(MasterCheckOp.ID_INPUT_TRANSACTION).dataModelType
      val masterSelection: Option[(String, Type)] = selectionMethod

      val opInfo: OperatorInfo = operatorInfo
    }

    context.jpContext.addClass(builder)
  }
}
