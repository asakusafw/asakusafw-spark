/*
 * Copyright 2011-2021 Asakusa Framework Team.
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
import com.asakusafw.spark.compiler.spi.{ OperatorCompiler, OperatorType }
import com.asakusafw.spark.runtime.fragment.user.join.BroadcastMasterCheckOperatorFragment
import com.asakusafw.spark.tools.asm._
import com.asakusafw.vocabulary.operator.{ MasterCheck => MasterCheckOp }

class BroadcastMasterCheckOperatorCompiler extends UserOperatorCompiler {

  override def support(
    operator: UserOperator)(
      implicit context: OperatorCompiler.Context): Boolean = {
    operator.annotationDesc.resolveClass == classOf[MasterCheckOp]
  }

  override def operatorType: OperatorType = OperatorType.ExtractType

  override def compile(
    operator: UserOperator)(
      implicit context: OperatorCompiler.Context): Type = {

    assert(support(operator),
      s"The operator type is not supported: ${operator.annotationDesc.resolveClass.getSimpleName}"
        + s" [${operator}]")
    assert(operator.inputs.size >= 2,
      "The size of inputs should be greater than or equals to 2: " +
        s"${operator.inputs.size} [${operator}]")

    assert(
      operator.outputs.forall(output =>
        output.dataModelType == operator.inputs(MasterCheckOp.ID_INPUT_TRANSACTION).dataModelType),
      s"All of output types should be the same as the transaction type: ${
        operator.outputs.map(_.dataModelType).mkString("(", ",", ")")
      } [${operator}]")

    val builder = new BroadcastMasterCheckOperatorFragmentClassBuilder(operator)

    context.addClass(builder)
  }
}

private class BroadcastMasterCheckOperatorFragmentClassBuilder(
  operator: UserOperator)(
    implicit context: OperatorCompiler.Context)
  extends JoinOperatorFragmentClassBuilder(
    operator.inputs(MasterCheckOp.ID_INPUT_TRANSACTION).dataModelType,
    operator,
    operator.inputs(MasterCheckOp.ID_INPUT_MASTER),
    operator.inputs(MasterCheckOp.ID_INPUT_TRANSACTION))(
    Option(
      new ClassSignatureBuilder()
        .newSuperclass {
          _.newClassType(classOf[BroadcastMasterCheckOperatorFragment[_, _]].asType) {
            _.newTypeArgument(
              SignatureVisitor.INSTANCEOF,
              operator.inputs(MasterCheckOp.ID_INPUT_MASTER).dataModelType)
              .newTypeArgument(
                SignatureVisitor.INSTANCEOF,
                operator.inputs(MasterCheckOp.ID_INPUT_TRANSACTION).dataModelType)
          }
        }),
    classOf[BroadcastMasterCheckOperatorFragment[_, _]].asType)
  with BroadcastJoin
  with MasterCheck {

  override def defCtor()(implicit mb: MethodBuilder): Unit = {
    val thisVar :: broadcastsVar :: fragmentVars = mb.argVars

    thisVar.push().invokeInit(
      superType,
      fragmentVars(MasterCheckOp.ID_OUTPUT_MISSED).push(),
      fragmentVars(MasterCheckOp.ID_OUTPUT_FOUND).push())
  }
}
