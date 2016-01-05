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
package com.asakusafw.spark.extensions.iterativebatch.compiler
package graph

import scala.collection.JavaConversions._

import org.objectweb.asm.Type

import com.asakusafw.lang.compiler.common.{ Diagnostic, DiagnosticException }
import com.asakusafw.lang.compiler.model.graph.ExternalInput
import com.asakusafw.lang.compiler.planning.SubPlan
import com.asakusafw.spark.compiler.`package`._
import com.asakusafw.spark.compiler.graph.{
  ComputeOnce,
  InputInstantiator,
  Instantiator,
  TemporaryInputClassBuilder
}
import com.asakusafw.spark.compiler.planning.{ IterativeInfo, SubPlanInfo }
import com.asakusafw.spark.compiler.spi.NodeCompiler

import com.asakusafw.spark.extensions.iterativebatch.compiler.spi.RoundAwareNodeCompiler

class TemporaryInputCompiler extends RoundAwareNodeCompiler {

  override def support(
    subplan: SubPlan)(
      implicit context: NodeCompiler.Context): Boolean = {
    subplan.getAttribute(classOf[SubPlanInfo]).getDriverType == SubPlanInfo.DriverType.INPUT && {
      val subPlanInfo = subplan.getAttribute(classOf[SubPlanInfo])
      val primaryOperator = subPlanInfo.getPrimaryOperator
      if (primaryOperator.isInstanceOf[ExternalInput]) {
        val operator = primaryOperator.asInstanceOf[ExternalInput]
        val inputFormatInfo = context.getInputFormatInfo(operator.getName, operator.getInfo)
        !context.options.useInputDirect || inputFormatInfo.isEmpty
      } else {
        false
      }
    }
  }

  override def instantiator: Instantiator = InputInstantiator

  override def compile(
    subplan: SubPlan)(
      implicit context: NodeCompiler.Context): Type = {
    assert(support(subplan), s"The subplan is not supported: ${subplan}")

    val subPlanInfo = subplan.getAttribute(classOf[SubPlanInfo])
    val primaryOperator = subPlanInfo.getPrimaryOperator
    val operator = primaryOperator.asInstanceOf[ExternalInput]

    if (IterativeInfo.getDeclared(operator).isIterative) {
      throw new DiagnosticException(
        Diagnostic.Level.ERROR,
        s"Iterative batch extension does not support iterative external inputs: ${subplan}")
    } else {

      val inputRef = context.addExternalInput(operator.getName, operator.getInfo)

      val iterativeInfo = IterativeInfo.get(subplan)

      val builder =
        iterativeInfo.getRecomputeKind match {
          case IterativeInfo.RecomputeKind.ALWAYS =>
            new TemporaryInputClassBuilder(
              operator,
              operator.getDataType.asType,
              inputRef.getPaths.toSeq.sorted)(
              subPlanInfo.getLabel,
              subplan.getOutputs.toSeq) with ComputeAlways
          case IterativeInfo.RecomputeKind.PARAMETER =>
            new TemporaryInputClassBuilder(
              operator,
              operator.getDataType.asType,
              inputRef.getPaths.toSeq.sorted)(
              subPlanInfo.getLabel,
              subplan.getOutputs.toSeq) with ComputeByParameter {

              override val parameters: Set[String] = iterativeInfo.getParameters.toSet
            }
          case IterativeInfo.RecomputeKind.NEVER =>
            new TemporaryInputClassBuilder(
              operator,
              operator.getDataType.asType,
              inputRef.getPaths.toSeq.sorted)(
              subPlanInfo.getLabel,
              subplan.getOutputs.toSeq) with ComputeOnce
        }

      context.addClass(builder)
    }
  }
}
