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

import com.asakusafw.lang.compiler.model.graph.UserOperator
import com.asakusafw.lang.compiler.planning.SubPlan
import com.asakusafw.spark.compiler.planning.{ SubPlanInfo, SubPlanInputInfo }
import com.asakusafw.spark.compiler.util.NumPartitions._
import com.asakusafw.spark.compiler.util.SparkIdioms._
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._
import com.asakusafw.spark.tools.asm4s._

object AggregateInstantiator extends Instantiator {

  override def newInstance(
    nodeType: Type,
    subplan: SubPlan,
    subplanToIdx: Map[SubPlan, Int])(
      vars: Instantiator.Vars)(
        implicit mb: MethodBuilder,
        context: Instantiator.Context): Var = {

    val primaryOperator =
      subplan.getAttribute(classOf[SubPlanInfo]).getPrimaryOperator.asInstanceOf[UserOperator]

    val input = primaryOperator.inputs.head

    val aggregate = pushNew(nodeType)
    aggregate.dup().invokeInit(
      buildSeq { builder =>
        for {
          subPlanInput <- subplan.getInputs
          inputInfo = subPlanInput.getAttribute(classOf[SubPlanInputInfo])
          if inputInfo.getInputType == SubPlanInputInfo.InputType.PARTITIONED
          prevSubPlanOutput <- subPlanInput.getOpposites
        } {
          val prevSubPlan = prevSubPlanOutput.getOwner
          val marker = prevSubPlanOutput.getOperator
          builder +=
            tuple2(
              vars.nodes.push().aload(ldc(subplanToIdx(prevSubPlan))),
              context.branchKeys.getField(marker))
        }
      },
      option(
        sortOrdering(
          input.dataModelRef.groupingTypes(input.getGroup.getGrouping),
          input.dataModelRef.orderingTypes(input.getGroup.getOrdering))),
      (if (input.getGroup.getGrouping.isEmpty) {
        partitioner(ldc(1))
      } else {
        partitioner(
          numPartitions(vars.jobContext.push())(
            subplan.findInput(input.getOpposites.head.getOwner)))
      }),
      vars.broadcasts.push(),
      vars.jobContext.push())
    aggregate.store()
  }
}
