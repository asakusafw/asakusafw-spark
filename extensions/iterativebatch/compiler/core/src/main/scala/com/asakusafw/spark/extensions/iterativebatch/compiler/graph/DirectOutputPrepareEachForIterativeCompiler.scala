/*
 * Copyright 2011-2019 Asakusa Framework Team.
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

import com.asakusafw.lang.compiler.extension.directio.{ DirectFileIoModels, OutputPattern }
import com.asakusafw.lang.compiler.model.graph.ExternalOutput
import com.asakusafw.lang.compiler.planning.SubPlan
import com.asakusafw.spark.compiler._
import com.asakusafw.spark.compiler.graph.{ CacheOnce, Instantiator }
import com.asakusafw.spark.compiler.planning.{ IterativeInfo, SubPlanInfo }
import com.asakusafw.spark.compiler.spi.NodeCompiler

import com.asakusafw.spark.extensions.iterativebatch.compiler.spi.RoundAwareNodeCompiler

class DirectOutputPrepareEachForIterativeCompiler extends RoundAwareNodeCompiler {

  override def support(
    subplan: SubPlan)(
      implicit context: NodeCompiler.Context): Boolean = {
    if (context.options.useOutputDirect) {
      val subPlanInfo = subplan.getAttribute(classOf[SubPlanInfo])
      val primaryOperator = subPlanInfo.getPrimaryOperator
      if (primaryOperator.isInstanceOf[ExternalOutput]) {
        val operator = primaryOperator.asInstanceOf[ExternalOutput]
        Option(operator.getInfo).map { info =>
          DirectFileIoModels.isSupported(info)
        }.getOrElse(false)
      } else {
        false
      }
    } else {
      false
    }
  }

  override def instantiator: Instantiator = DirectOutputPrepareEachForIterativeInstantiator

  override def compile(
    subplan: SubPlan)(
      implicit context: NodeCompiler.Context): Type = {
    assert(support(subplan), s"The subplan is not supported: ${subplan}")

    val subPlanInfo = subplan.getAttribute(classOf[SubPlanInfo])
    val primaryOperator = subPlanInfo.getPrimaryOperator
    assert(primaryOperator.isInstanceOf[ExternalOutput],
      s"The primary operator should be external output: ${primaryOperator} [${subplan}]")
    val operator = primaryOperator.asInstanceOf[ExternalOutput]

    val model = DirectFileIoModels.resolve(operator.getInfo)
    val dataModelRef = operator.getOperatorPort.dataModelRef
    val pattern = OutputPattern.compile(dataModelRef, model.getResourcePattern, model.getOrder)

    val iterativeInfo = IterativeInfo.get(subplan)

    val builder = if (pattern.isGatherRequired) {
      iterativeInfo.getRecomputeKind match {
        case IterativeInfo.RecomputeKind.ALWAYS =>
          new DirectOutputPrepareGroupEachForIterativeClassBuilder(
            operator)(
            pattern,
            model)(
            subplan.label) with CacheAlways
        case IterativeInfo.RecomputeKind.PARAMETER =>
          new DirectOutputPrepareGroupEachForIterativeClassBuilder(
            operator)(
            pattern,
            model)(
            subplan.label) with CacheByParameter {

            override val parameters: Set[String] = iterativeInfo.getParameters.toSet
          }
        case IterativeInfo.RecomputeKind.NEVER =>
          new DirectOutputPrepareGroupEachForIterativeClassBuilder(
            operator)(
            pattern,
            model)(
            subplan.label) with CacheOnce
      }
    } else {
      iterativeInfo.getRecomputeKind match {
        case IterativeInfo.RecomputeKind.ALWAYS =>
          new DirectOutputPrepareEachFlatForIterativeClassBuilder(
            operator)(
            model)(
            subplan.label) with CacheAlways
        case IterativeInfo.RecomputeKind.PARAMETER =>
          new DirectOutputPrepareEachFlatForIterativeClassBuilder(
            operator)(
            model)(
            subplan.label) with CacheByParameter {

            override val parameters: Set[String] = iterativeInfo.getParameters.toSet
          }
        case IterativeInfo.RecomputeKind.NEVER =>
          new DirectOutputPrepareEachFlatForIterativeClassBuilder(
            operator)(
            model)(
            subplan.label) with CacheOnce
      }
    }

    context.addClass(builder)
  }
}
