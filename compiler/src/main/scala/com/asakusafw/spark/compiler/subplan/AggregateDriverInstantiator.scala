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

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConversions._
import scala.concurrent.Future

import org.apache.spark.rdd.RDD
import org.objectweb.asm.Type

import com.asakusafw.lang.compiler.model.graph.UserOperator
import com.asakusafw.lang.compiler.planning.SubPlan
import com.asakusafw.spark.compiler.planning.{ SubPlanInfo, SubPlanInputInfo }
import com.asakusafw.spark.runtime.driver.ShuffleKey
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._

object AggregateDriverInstantiator
  extends Instantiator
  with NumPartitions
  with ScalaIdioms
  with SparkIdioms {

  override def newInstance(
    driverType: Type,
    subplan: SubPlan)(
      mb: MethodBuilder,
      vars: Instantiator.Vars,
      nextLocal: AtomicInteger)(
        implicit context: Instantiator.Context): Var = {
    import mb._ // scalastyle:ignore

    val primaryOperator =
      subplan.getAttribute(classOf[SubPlanInfo]).getPrimaryOperator.asInstanceOf[UserOperator]

    assert(primaryOperator.inputs.size == 1,
      s"The size of inputs should be 1: ${primaryOperator.inputs.size}")
    val input = primaryOperator.inputs.head

    val aggregateDriver = pushNew(driverType)
    aggregateDriver.dup().invokeInit(
      vars.sc.push(),
      vars.hadoopConf.push(),
      buildSeq(mb) { builder =>
        for {
          subPlanInput <- subplan.getInputs
          inputInfo = subPlanInput.getAttribute(classOf[SubPlanInputInfo])
          if inputInfo.getInputType == SubPlanInputInfo.InputType.PARTITIONED
          prevSubPlanOutput <- subPlanInput.getOpposites
          marker = prevSubPlanOutput.getOperator
        } {
          builder +=
            applyMap(mb)(
              vars.rdds.push(),
              context.branchKeys.getField(mb, marker))
            .cast(classOf[Future[RDD[(ShuffleKey, _)]]].asType)
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
      vars.broadcasts.push())
    aggregateDriver.store(nextLocal.getAndAdd(aggregateDriver.size))
  }
}
