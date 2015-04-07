package com.asakusafw.spark.compiler
package operator
package user
package join

import org.objectweb.asm.Type

import com.asakusafw.lang.compiler.model.graph.UserOperator
import com.asakusafw.spark.compiler.spi.OperatorType
import com.asakusafw.spark.compiler.subplan.BroadcastIdsClassBuilder
import com.asakusafw.vocabulary.operator.{ MasterCheck => MasterCheckOp }
import com.asakusafw.spark.tools.asm._

class ShuffledMasterCheckOperatorCompiler extends UserOperatorCompiler {

  override def support(operator: UserOperator)(implicit context: Context): Boolean = {
    val operatorInfo = new OperatorInfo(operator)(context.jpContext)
    import operatorInfo._
    annotationDesc.resolveClass == classOf[MasterCheckOp]
  }

  override def operatorType: OperatorType = OperatorType.CoGroupType

  override def compile(operator: UserOperator)(implicit context: Context): Type = {

    val operatorInfo = new OperatorInfo(operator)(context.jpContext)
    import operatorInfo._

    assert(support(operator),
      s"The operator type is not supported: ${annotationDesc.resolveClass.getSimpleName}")
    assert(inputs.size == 2, // FIXME to take multiple inputs for side data?
      s"The size of inputs should be 2: ${inputs.size}")

    assert(
      outputs.forall { output =>
        output.dataModelType == inputs(MasterCheckOp.ID_INPUT_TRANSACTION).dataModelType
      },
      s"All of output types should be the same as the transaction type: ${
        outputs.map(_.dataModelType).mkString("(", ",", ")")
      }")

    val builder = new JoinOperatorFragmentClassBuilder(
      context.flowId,
      classOf[Seq[Iterable[_]]].asType,
      implementationClassType,
      outputs) with ShuffledJoin with MasterCheck {

      val broadcastIds: BroadcastIdsClassBuilder = context.broadcastIds

      val masterType: Type = inputs(MasterCheckOp.ID_INPUT_MASTER).dataModelType
      val txType: Type = inputs(MasterCheckOp.ID_INPUT_TRANSACTION).dataModelType
      val masterSelection: Option[(String, Type)] = selectionMethod

      val opInfo: OperatorInfo = operatorInfo
    }

    context.jpContext.addClass(builder)
  }
}
