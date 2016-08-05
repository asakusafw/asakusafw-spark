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
package graph

import scala.collection.JavaConversions._

import org.objectweb.asm.Type

import com.asakusafw.lang.compiler.planning.SubPlan
import com.asakusafw.spark.compiler.planning.SubPlanInfo

import com.asakusafw.spark.compiler.spi.NodeCompiler

class ExtractCompiler extends NodeCompiler {

  override def support(
    subplan: SubPlan)(
      implicit context: NodeCompiler.Context): Boolean = {
    subplan.getAttribute(classOf[SubPlanInfo]).getDriverType == SubPlanInfo.DriverType.EXTRACT
  }

  override def instantiator: Instantiator = ExtractInstantiator

  override def compile(
    subplan: SubPlan)(
      implicit context: NodeCompiler.Context): Type = {
    assert(support(subplan), s"The subplan is not supported: ${subplan}")

    val subPlanInfo = subplan.getAttribute(classOf[SubPlanInfo])
    val primaryInputs = subPlanInfo.getPrimaryInputs.toSet[SubPlan.Input]
    assert(primaryInputs.size == 1,
      s"The size of primary inputs should be 1: ${primaryInputs.size} [${subplan}]")

    val marker = primaryInputs.head.getOperator

    val builder =
      new ExtractClassBuilder(
        marker)(
        subPlanInfo.getLabel,
        subplan.getOutputs.toSeq) with ComputeOnce

    context.addClass(builder)
  }
}
