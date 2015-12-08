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
package com.asakusafw.spark.extensions.iterativebatch.compiler.graph

import scala.collection.JavaConversions._

import org.objectweb.asm.Type

import com.asakusafw.lang.compiler.model.graph.UserOperator
import com.asakusafw.lang.compiler.planning.SubPlan
import com.asakusafw.spark.compiler.graph.{
  CoGroupClassBuilder,
  CoGroupInstantiator,
  ComputeOnce,
  Instantiator
}
import com.asakusafw.spark.compiler.planning.{ IterativeInfo, SubPlanInfo }
import com.asakusafw.spark.compiler.spi.NodeCompiler

import com.asakusafw.spark.extensions.iterativebatch.compiler.spi.RoundAwareNodeCompiler

class CoGroupCompiler extends RoundAwareNodeCompiler {

  override def support(
    subplan: SubPlan)(
      implicit context: NodeCompiler.Context): Boolean = {
    subplan.getAttribute(classOf[SubPlanInfo]).getDriverType == SubPlanInfo.DriverType.COGROUP
  }

  override def instantiator: Instantiator = CoGroupInstantiator

  override def compile(
    subplan: SubPlan)(
      implicit context: NodeCompiler.Context): Type = {
    assert(support(subplan), s"The subplan is not supported: ${subplan}")

    val subPlanInfo = subplan.getAttribute(classOf[SubPlanInfo])
    val primaryOperator = subPlanInfo.getPrimaryOperator
    assert(primaryOperator.isInstanceOf[UserOperator],
      s"The primary operator should be user operator: ${primaryOperator}")
    val operator = primaryOperator.asInstanceOf[UserOperator]

    val iterativeInfo = IterativeInfo.get(subplan)

    val builder =
      iterativeInfo.getRecomputeKind match {
        case IterativeInfo.RecomputeKind.ALWAYS =>
          new CoGroupClassBuilder(
            operator)(
            subPlanInfo.getLabel,
            subplan.getOutputs.toSeq) with ComputeAlways
        case IterativeInfo.RecomputeKind.PARAMETER =>
          new CoGroupClassBuilder(
            operator)(
            subPlanInfo.getLabel,
            subplan.getOutputs.toSeq) with ComputeByParameter {

            override val parameters: Set[String] = iterativeInfo.getParameters.toSet
          }
        case IterativeInfo.RecomputeKind.NEVER =>
          new CoGroupClassBuilder(
            operator)(
            subPlanInfo.getLabel,
            subplan.getOutputs.toSeq) with ComputeOnce
      }

    context.addClass(builder)
  }
}
