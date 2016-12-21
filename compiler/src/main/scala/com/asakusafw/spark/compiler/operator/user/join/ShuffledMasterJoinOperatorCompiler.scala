/*
 * Copyright 2011-2016 Asakusa Framework Team.
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
package operator.user
package join

import org.objectweb.asm.Type
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.lang.compiler.model.graph.UserOperator
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.spark.compiler.spi.{ OperatorCompiler, OperatorType }
import com.asakusafw.spark.runtime.fragment.user.join.ShuffledMasterJoinOperatorFragment
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._
import com.asakusafw.vocabulary.operator.{ MasterJoin => MasterJoinOp }

class ShuffledMasterJoinOperatorCompiler extends UserOperatorCompiler {

  override def support(
    operator: UserOperator)(
      implicit context: OperatorCompiler.Context): Boolean = {
    operator.annotationDesc.resolveClass == classOf[MasterJoinOp]
  }

  override def operatorType: OperatorType = OperatorType.CoGroupType

  override def compile(
    operator: UserOperator)(
      implicit context: OperatorCompiler.Context): Type = {

    assert(support(operator),
      s"The operator type is not supported: ${operator.annotationDesc.resolveClass.getSimpleName}"
        + s" [${operator}]")
    assert(operator.inputs.size == 2, // FIXME to take multiple inputs for side data?
      s"The size of inputs should be 2: ${operator.inputs.size} [${operator}]")
    assert(operator.outputs.size == 2,
      s"The size of outputs should be greater than 2: ${operator.outputs.size} [${operator}]")

    assert(operator.outputs(MasterJoinOp.ID_OUTPUT_MISSED).dataModelType
      == operator.inputs(MasterJoinOp.ID_INPUT_TRANSACTION).dataModelType,
      s"The `missed` output type should be the same as the transaction type: ${
        operator.outputs(MasterJoinOp.ID_OUTPUT_MISSED).dataModelType
      } [${operator}]")

    val builder = new ShuffledMasterJoinOperatorFragmentClassBuilder(operator)

    context.addClass(builder)
  }
}

private class ShuffledMasterJoinOperatorFragmentClassBuilder(
  operator: UserOperator)(
    implicit context: OperatorCompiler.Context)
  extends JoinOperatorFragmentClassBuilder(
    classOf[IndexedSeq[Iterator[_]]].asType,
    operator,
    operator.inputs(MasterJoinOp.ID_INPUT_MASTER),
    operator.inputs(MasterJoinOp.ID_INPUT_TRANSACTION))(
    Option(
      new ClassSignatureBuilder()
        .newSuperclass {
          _.newClassType(classOf[ShuffledMasterJoinOperatorFragment[_, _, _]].asType) {
            _.newTypeArgument(
              SignatureVisitor.INSTANCEOF,
              operator.inputs(MasterJoinOp.ID_INPUT_MASTER).dataModelType)
              .newTypeArgument(
                SignatureVisitor.INSTANCEOF,
                operator.inputs(MasterJoinOp.ID_INPUT_TRANSACTION).dataModelType)
              .newTypeArgument(
                SignatureVisitor.INSTANCEOF,
                operator.outputs(MasterJoinOp.ID_OUTPUT_JOINED).dataModelType)
          }
        }),
    classOf[ShuffledMasterJoinOperatorFragment[_, _, _]].asType)
  with ShuffledJoin
  with MasterJoin {

  override def defCtor()(implicit mb: MethodBuilder): Unit = {
    val thisVar :: broadcastsVar :: fragmentVars = mb.argVars

    thisVar.push().invokeInit(
      superType,
      fragmentVars(MasterJoinOp.ID_OUTPUT_MISSED).push(),
      fragmentVars(MasterJoinOp.ID_OUTPUT_JOINED).push(),
      pushNew0(joinedType).asType(classOf[DataModel[_]].asType))
  }
}
