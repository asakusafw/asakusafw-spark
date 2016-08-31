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

import com.asakusafw.lang.compiler.extension.directio.{ DirectFileIoModels, OutputPattern }
import com.asakusafw.lang.compiler.planning.SubPlan
import com.asakusafw.lang.compiler.model.graph.ExternalOutput
import com.asakusafw.spark.compiler.planning.{ SubPlanInfo, SubPlanInputInfo }
import com.asakusafw.spark.compiler.util.NumPartitions._
import com.asakusafw.spark.compiler.util.SparkIdioms._
import com.asakusafw.spark.runtime.graph.Action
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._
import com.asakusafw.spark.tools.asm4s._

object DirectOutputPrepareInstantiator extends Instantiator {

  override def newInstance(
    nodeType: Type,
    subplan: SubPlan,
    subplanToIdx: Map[SubPlan, Int])(
      vars: Instantiator.Vars)(
        implicit mb: MethodBuilder,
        context: Instantiator.Context): Var = {

    val subPlanInfo = subplan.getAttribute(classOf[SubPlanInfo])
    val primaryOperator = subPlanInfo.getPrimaryOperator
    assert(primaryOperator.isInstanceOf[ExternalOutput],
      s"The primary operator should be external output: ${primaryOperator} [${subplan}]")
    val operator = primaryOperator.asInstanceOf[ExternalOutput]

    val model = DirectFileIoModels.resolve(operator.getInfo)
    val dataModelRef = operator.getOperatorPort.dataModelRef
    val pattern = OutputPattern.compile(dataModelRef, model.getResourcePattern, model.getOrder)

    val output = pushNew(nodeType)
    if (pattern.isGatherRequired) {
      output.dup().invokeInit(
        vars.setup.get.push().asType(classOf[Action[Unit]].asType),
        buildSeq { builder =>
          for {
            subPlanInput <- subplan.getInputs
            inputInfo <- Option(subPlanInput.getAttribute(classOf[SubPlanInputInfo]))
            if inputInfo.getInputType == SubPlanInputInfo.InputType.PREPARE_EXTERNAL_OUTPUT
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
        partitioner(
          numPartitions(vars.sc.push())(
            subplan.findInput(primaryOperator.inputs.head.getOpposites.head.getOwner))),
        vars.sc.push())
    } else {
      output.dup().invokeInit(
        vars.setup.get.push().asType(classOf[Action[Unit]].asType),
        buildSeq { builder =>
          for {
            subPlanInput <- subplan.getInputs
            inputInfo <- Option(subPlanInput.getAttribute(classOf[SubPlanInputInfo]))
            if inputInfo.getInputType == SubPlanInputInfo.InputType.PREPARE_EXTERNAL_OUTPUT
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
        vars.sc.push())
    }
    output.store()
  }
}
