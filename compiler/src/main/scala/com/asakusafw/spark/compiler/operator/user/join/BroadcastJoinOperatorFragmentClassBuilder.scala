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
package operator
package user
package join

import org.objectweb.asm.Type

import com.asakusafw.spark.compiler.spi.OperatorCompiler
import com.asakusafw.lang.compiler.model.graph.{ OperatorInput, OperatorOutput, UserOperator }

abstract class BroadcastJoinOperatorFragmentClassBuilder(
  val operator: UserOperator,
  val masterInput: OperatorInput,
  val txInput: OperatorInput)(
    implicit val context: OperatorCompiler.Context)
  extends JoinOperatorFragmentClassBuilder(
    txInput.dataModelType,
    operator.implementationClass.asType,
    operator.outputs)
  with BroadcastJoin {

  val masterType: Type = masterInput.dataModelType
  val txType: Type = txInput.dataModelType
  val masterSelection: Option[(String, Type)] = operator.selectionMethod
}
