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
package subplan

import org.objectweb.asm.Type

import com.asakusafw.lang.compiler.model.graph.ExternalOutput
import com.asakusafw.lang.compiler.planning.SubPlan
import com.asakusafw.spark.compiler.planning.SubPlanInfo
import com.asakusafw.spark.compiler.spi.SubPlanCompiler

class OutputSubPlanCompiler extends SubPlanCompiler {

  def of: SubPlanInfo.DriverType = SubPlanInfo.DriverType.OUTPUT

  override def instantiator: Instantiator = OutputDriverInstantiator

  override def compile(
    subplan: SubPlan)(
      implicit context: SubPlanCompiler.Context): Type = {
    val subPlanInfo = subplan.getAttribute(classOf[SubPlanInfo])
    val primaryOperator = subPlanInfo.getPrimaryOperator
    assert(primaryOperator.isInstanceOf[ExternalOutput],
      s"The dominant operator should be external output: ${primaryOperator}")
    val operator = primaryOperator.asInstanceOf[ExternalOutput]

    context.addExternalOutput(
      operator.getName, operator.getInfo,
      Seq(context.options.getRuntimeWorkingPath(s"${operator.getName}/part-*")))

    val builder =
      new OutputDriverClassBuilder(
        operator)(
        subPlanInfo.getLabel)

    context.addClass(builder)
  }
}
