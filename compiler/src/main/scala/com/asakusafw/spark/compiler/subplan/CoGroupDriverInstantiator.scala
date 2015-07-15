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

import org.apache.spark.{ HashPartitioner, Partitioner }
import org.apache.spark.rdd.RDD
import org.objectweb.asm.Type

import com.asakusafw.lang.compiler.model.graph.{ Group, OperatorOutput }
import com.asakusafw.lang.compiler.planning.SubPlan
import com.asakusafw.spark.compiler.ordering.{
  GroupingOrderingClassBuilder,
  SortOrderingClassBuilder
}
import com.asakusafw.spark.compiler.planning.{ SubPlanInfo, SubPlanInputInfo }
import com.asakusafw.spark.runtime.driver.ShuffleKey
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._

object CoGroupDriverInstantiator
  extends Instantiator
  with NumPartitions
  with ScalaIdioms {

  override def newInstance(
    driverType: Type,
    subplan: SubPlan)(
      mb: MethodBuilder,
      vars: Instantiator.Vars,
      nextLocal: AtomicInteger)(
        implicit context: SparkClientCompiler.Context): Var = {
    import mb._ // scalastyle:ignore

    val primaryOperator = subplan.getAttribute(classOf[SubPlanInfo]).getPrimaryOperator

    val properties = primaryOperator.inputs.map { input =>
      val dataModelRef = input.dataModelRef
      input.getGroup.getGrouping.map { grouping =>
        dataModelRef.findProperty(grouping).getType.asType
      }.toSeq
    }.toSet
    assert(properties.size == 1,
      s"The grouping of all inputs should be the same: ${
        properties.map(_.mkString("(", ",", ")")).mkString("(", ",", ")")
      }")

    val partitioner = pushNew(classOf[HashPartitioner].asType)
    partitioner.dup().invokeInit(
      if (properties.head.isEmpty) {
        ldc(1)
      } else {
        numPartitions(
          mb,
          vars.sc.push())(
            subplan.findInput(primaryOperator.inputs.head.getOpposites.head.getOwner))
      })
    val partitionerVar = partitioner.store(nextLocal.getAndAdd(partitioner.size))

    val cogroupDriver = pushNew(driverType)
    cogroupDriver.dup().invokeInit(
      vars.sc.push(),
      vars.hadoopConf.push(),
      vars.broadcasts.push(), {
        // Seq[(Seq[RDD[(K, _)]], Option[Ordering[ShuffleKey]])]
        buildSeq(mb) { builder =>
          for {
            input <- primaryOperator.getInputs
          } {
            builder +=
              tuple2(mb)(
                buildSeq(mb) { builder =>
                  for {
                    opposite <- input.getOpposites.toSet[OperatorOutput]
                    subPlanInput <- Option(subplan.findInput(opposite.getOwner))
                    inputInfo <- Option(subPlanInput.getAttribute(classOf[SubPlanInputInfo]))
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
                option(mb)({
                  val dataModelRef =
                    context.jpContext.getDataModelLoader.load(input.getDataType)
                  pushNew0(
                    SortOrderingClassBuilder.getOrCompile(
                      input.getGroup.getGrouping.map { grouping =>
                        dataModelRef.findProperty(grouping).getType.asType
                      },
                      input.getGroup.getOrdering.map { ordering =>
                        (dataModelRef.findProperty(ordering.getPropertyName).getType.asType,
                          ordering.getDirection == Group.Direction.ASCENDANT)
                      }))
                }))
          }
        }
      }, {
        // ShuffleKey.GroupingOrdering
        pushNew0(
          GroupingOrderingClassBuilder.getOrCompile(properties.head))
          .asType(classOf[Ordering[ShuffleKey]].asType)
      }, {
        // Partitioner
        partitionerVar.push().asType(classOf[Partitioner].asType)
      })
    cogroupDriver.store(nextLocal.getAndAdd(cogroupDriver.size))
  }
}
