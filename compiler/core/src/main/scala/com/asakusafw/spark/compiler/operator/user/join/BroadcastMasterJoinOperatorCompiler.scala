/*
 * Copyright 2011-2015 Asakusa Framework Team.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.asakusafw.spark.compiler
package operator
package user
package join

import scala.collection.JavaConversions._
import scala.collection.mutable

import org.objectweb.asm.Type

import com.asakusafw.lang.compiler.analyzer.util.JoinedModelUtil
import com.asakusafw.lang.compiler.api.JobflowProcessor.{ Context => JPContext }
import com.asakusafw.lang.compiler.model.graph.{ MarkerOperator, OperatorInput, UserOperator }
import com.asakusafw.spark.compiler.spi.OperatorType
import com.asakusafw.spark.compiler.subplan.BroadcastIds
import com.asakusafw.vocabulary.operator.{ MasterJoin => MasterJoinOp }

class BroadcastMasterJoinOperatorCompiler extends UserOperatorCompiler {

  override def support(operator: UserOperator)(implicit context: Context): Boolean = {
    val operatorInfo = new OperatorInfo(operator)(context.jpContext)
    import operatorInfo._
    annotationDesc.resolveClass == classOf[MasterJoinOp]
  }

  override def operatorType: OperatorType = OperatorType.MapType

  override def compile(operator: UserOperator)(implicit context: Context): Type = {

    val operatorInfo = new OperatorInfo(operator)(context.jpContext)
    import operatorInfo._

    assert(support(operator),
      s"The operator type is not supported: ${annotationDesc.resolveClass.getSimpleName}")
    assert(inputs.size == 2, // FIXME to take multiple inputs for side data?
      s"The size of inputs should be 2: ${inputs.size}")
    assert(outputs.size == 2,
      s"The size of outputs should be greater than 2: ${outputs.size}")

    assert(outputs(MasterJoinOp.ID_OUTPUT_MISSED).dataModelType
      == inputs(MasterJoinOp.ID_INPUT_TRANSACTION).dataModelType,
      s"The `missed` output type should be the same as the transaction type: ${
        outputs(MasterJoinOp.ID_OUTPUT_MISSED).dataModelType
      }")

    val builder = new JoinOperatorFragmentClassBuilder(
      context.flowId,
      inputs(MasterJoinOp.ID_INPUT_TRANSACTION).dataModelType,
      implementationClassType,
      outputs) with BroadcastJoin with MasterJoin {

      val jpContext: JPContext = context.jpContext

      val broadcastIds: BroadcastIds = context.broadcastIds

      lazy val masterInput: OperatorInput = inputs(MasterJoinOp.ID_INPUT_MASTER)
      lazy val txInput: OperatorInput = inputs(MasterJoinOp.ID_INPUT_TRANSACTION)

      lazy val masterType: Type = masterInput.dataModelType
      lazy val txType: Type = dataModelType
      lazy val masterSelection: Option[(String, Type)] = selectionMethod

      val opInfo: OperatorInfo = operatorInfo

      val mappings = JoinedModelUtil.getPropertyMappings(context.jpContext.getClassLoader, operator).toSeq
    }

    context.jpContext.addClass(builder)
  }
}
