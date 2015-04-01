package com.asakusafw.spark.compiler
package operator
package user
package join

import scala.collection.JavaConversions._

import org.objectweb.asm.Type

import com.asakusafw.lang.compiler.analyzer.util.JoinedModelUtil
import com.asakusafw.lang.compiler.model.graph.UserOperator
import com.asakusafw.spark.compiler.spi.OperatorType
import com.asakusafw.vocabulary.operator.{ MasterJoin => MasterJoinOp }

class BroadcastMasterJoinOperatorCompiler extends UserOperatorCompiler {

  override def support(operator: UserOperator)(implicit context: Context): Boolean = {
    val operatorInfo = new OperatorInfo(operator)(context.jpContext)
    import operatorInfo._
    annotationDesc.resolveClass == classOf[MasterJoinOp]
  }

  override def operatorType: OperatorType = OperatorType.MapType

  override def compile(operator: UserOperator)(implicit context: Context): Type = {
    assert(support(operator))

    val operatorInfo = new OperatorInfo(operator)(context.jpContext)
    import operatorInfo._

    assert(inputs.size >= 2)
    assert(outputs.size == 2)

    assert(outputs(MasterJoinOp.ID_OUTPUT_MISSED).dataModelType
      == inputs(MasterJoinOp.ID_INPUT_TRANSACTION).dataModelType)

    val builder = new JoinOperatorFragmentClassBuilder(
      context.flowId,
      implementationClassType,
      outputs) with BroadcastJoin with MasterJoin {

      val masterType: Type = inputs(MasterJoinOp.ID_INPUT_MASTER).dataModelType
      val txType: Type = inputs(MasterJoinOp.ID_INPUT_TRANSACTION).dataModelType
      val masterSelection: Option[(String, Type)] = selectionMethod

      val opInfo: OperatorInfo = operatorInfo

      val mappings = JoinedModelUtil.getPropertyMappings(context.jpContext.getClassLoader, operator).toSeq
    }

    context.jpContext.addClass(builder)
  }
}
