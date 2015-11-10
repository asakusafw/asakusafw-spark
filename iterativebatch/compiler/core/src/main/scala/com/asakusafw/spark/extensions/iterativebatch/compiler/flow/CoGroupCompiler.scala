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
package com.asakusafw.spark.extensions.iterativebatch.compiler
package flow

import scala.collection.JavaConversions._

import org.objectweb.asm.Type

import com.asakusafw.lang.compiler.model.graph.UserOperator
import com.asakusafw.lang.compiler.planning.SubPlan
import com.asakusafw.spark.compiler.planning.SubPlanInfo

import com.asakusafw.spark.extensions.iterativebatch.compiler.spi.NodeCompiler

class CoGroupCompiler extends NodeCompiler {

  override def of: SubPlanInfo.DriverType = SubPlanInfo.DriverType.COGROUP

  override def instantiator: Instantiator = CoGroupInstantiator

  override def compile(
    subplan: SubPlan)(
      implicit context: NodeCompiler.Context): Type = {
    val subPlanInfo = subplan.getAttribute(classOf[SubPlanInfo])
    val primaryOperator = subPlanInfo.getPrimaryOperator
    assert(primaryOperator.isInstanceOf[UserOperator],
      s"The dominant operator should be user operator: ${primaryOperator}")
    val operator = primaryOperator.asInstanceOf[UserOperator]

    val builder =
      new CoGroupClassBuilder(
        operator)(
        subPlanInfo.getLabel,
        subplan.getOutputs.toSeq)

    context.addClass(builder)
  }
}
