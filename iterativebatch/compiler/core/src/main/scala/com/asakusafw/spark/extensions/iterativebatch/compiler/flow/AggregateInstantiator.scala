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

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConversions._

import org.objectweb.asm.Type

import com.asakusafw.lang.compiler.model.graph.UserOperator
import com.asakusafw.lang.compiler.planning.SubPlan
import com.asakusafw.spark.compiler.`package`._
import com.asakusafw.spark.compiler.planning.{ SubPlanInfo, SubPlanInputInfo }
import com.asakusafw.spark.compiler.subplan.NumPartitions._
import com.asakusafw.spark.compiler.util.ScalaIdioms._
import com.asakusafw.spark.compiler.util.SparkIdioms._
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._

object AggregateInstantiator extends Instantiator {

  override def newInstance(
    nodeType: Type,
    subplan: SubPlan,
    subplanToIdx: Map[SubPlan, Int])(
      mb: MethodBuilder,
      vars: Instantiator.Vars,
      nextLocal: AtomicInteger)(
        implicit context: Instantiator.Context): Var = {
    import mb._ // scalastyle:ignore

    val primaryOperator =
      subplan.getAttribute(classOf[SubPlanInfo]).getPrimaryOperator.asInstanceOf[UserOperator]

    assert(primaryOperator.inputs.size == 1,
      s"The size of inputs should be 1: ${primaryOperator.inputs.size} [${subplan}]")
    val input = primaryOperator.inputs.head

    val aggregate = pushNew(nodeType)
    aggregate.dup().invokeInit(
      buildSeq(mb) { builder =>
        for {
          subPlanInput <- subplan.getInputs
          inputInfo = subPlanInput.getAttribute(classOf[SubPlanInputInfo])
          if inputInfo.getInputType == SubPlanInputInfo.InputType.PARTITIONED
          prevSubPlanOutput <- subPlanInput.getOpposites
        } {
          val prevSubPlan = prevSubPlanOutput.getOwner
          val marker = prevSubPlanOutput.getOperator
          builder +=
            tuple2(mb)(
              vars.nodes.push().aload(ldc(subplanToIdx(prevSubPlan))),
              context.branchKeys.getField(mb, marker))
        }
      },
      option(mb)(
        sortOrdering(mb)(
          input.dataModelRef.groupingTypes(input.getGroup.getGrouping),
          input.dataModelRef.orderingTypes(input.getGroup.getOrdering))),
      (if (input.getGroup.getGrouping.isEmpty) {
        partitioner(mb)(ldc(1))
      } else {
        partitioner(mb)(
          numPartitions(mb)(vars.sc.push())(
            subplan.findInput(input.getOpposites.head.getOwner)))
      }),
      vars.broadcasts.push(),
      vars.sc.push())
    aggregate.store(nextLocal.getAndAdd(aggregate.size))
  }
}
