/*
 * Copyright 2011-2018 Asakusa Framework Team.
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
import com.asakusafw.spark.compiler.planning.SubPlanInputInfo
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._
import com.asakusafw.spark.tools.asm4s._

object ExtractInstantiator extends Instantiator {

  override def newInstance(
    nodeType: Type,
    subplan: SubPlan,
    subplanToIdx: Map[SubPlan, Int])(
      vars: Instantiator.Vars)(
        implicit mb: MethodBuilder,
        context: Instantiator.Context): Var = {

    val extract = pushNew(nodeType)
    extract.dup().invokeInit(
      buildSeq { builder =>
        for {
          subPlanInput <- subplan.getInputs.toSet[SubPlan.Input]
          inputInfo <- Option(subPlanInput.getAttribute(classOf[SubPlanInputInfo]))
          if inputInfo.getInputType == SubPlanInputInfo.InputType.DONT_CARE
          prevSubPlanOutput <- subPlanInput.getOpposites.toSeq
        } {
          val prevSubPlan = prevSubPlanOutput.getOwner
          val marker = prevSubPlanOutput.getOperator
          builder +=
            tuple2(
              vars.nodes.push().aload(ldc(subplanToIdx(prevSubPlan))),
              context.branchKeys.getField(marker))
        }
      },
      vars.broadcasts.push(),
      vars.jobContext.push())
    extract.store()
  }
}
