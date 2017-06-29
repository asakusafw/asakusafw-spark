/*
 * Copyright 2011-2017 Asakusa Framework Team.
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
package graph

import scala.collection.JavaConversions._

import org.objectweb.asm.Type

import com.asakusafw.lang.compiler.model.graph.ExternalInput
import com.asakusafw.lang.compiler.planning.SubPlan
import com.asakusafw.spark.compiler.planning.SubPlanInfo
import com.asakusafw.spark.compiler.spi.NodeCompiler

class TemporaryInputCompiler extends NodeCompiler {

  override def support(
    subplan: SubPlan)(
      implicit context: NodeCompiler.Context): Boolean = {
    val subPlanInfo = subplan.getAttribute(classOf[SubPlanInfo])
    val primaryOperator = subPlanInfo.getPrimaryOperator
    if (primaryOperator.isInstanceOf[ExternalInput]) {
      val operator = primaryOperator.asInstanceOf[ExternalInput]
      if (context.options.useInputDirect) {
        Option(operator.getInfo).flatMap { info =>
          context.getInputFormatInfo(operator.getName, info)
        }.isEmpty
      } else {
        true
      }
    } else {
      false
    }
  }

  override def instantiator: Instantiator = InputInstantiator

  override def compile(
    subplan: SubPlan)(
      implicit context: NodeCompiler.Context): Type = {
    assert(support(subplan), s"The subplan is not supported: ${subplan}")

    val subPlanInfo = subplan.getAttribute(classOf[SubPlanInfo])
    val primaryOperator = subPlanInfo.getPrimaryOperator

    assert(primaryOperator.isInstanceOf[ExternalInput],
      s"The primary operator should be external input: ${primaryOperator} [${subplan}]")
    val operator = primaryOperator.asInstanceOf[ExternalInput]

    val inputRef = context.addExternalInput(operator.getName, operator.getInfo)
    val builder =
      new TemporaryInputClassBuilder(
        operator,
        operator.getDataType.asType,
        inputRef.getPaths.toSeq.sorted)(
        subplan.label,
        subplan.getOutputs.toSeq) with CacheOnce

    context.addClass(builder)
  }
}
