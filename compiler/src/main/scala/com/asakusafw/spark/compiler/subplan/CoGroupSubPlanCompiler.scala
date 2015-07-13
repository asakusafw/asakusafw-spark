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
import scala.collection.mutable
import scala.concurrent.Future
import scala.reflect.NameTransformer

import org.apache.spark.{ HashPartitioner, Partitioner }
import org.apache.spark.rdd.RDD
import org.objectweb.asm.Type
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.lang.compiler.model.graph.{ Group, OperatorOutput, UserOperator }
import com.asakusafw.lang.compiler.planning.SubPlan
import com.asakusafw.spark.compiler.operator.OperatorInfo
import com.asakusafw.spark.compiler.ordering.{
  GroupingOrderingClassBuilder,
  SortOrderingClassBuilder
}
import com.asakusafw.spark.compiler.planning.{ SubPlanInfo, SubPlanInputInfo }
import com.asakusafw.spark.compiler.spi.SubPlanCompiler
import com.asakusafw.spark.runtime.driver.ShuffleKey
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._

class CoGroupSubPlanCompiler extends SubPlanCompiler {

  def of: SubPlanInfo.DriverType = SubPlanInfo.DriverType.COGROUP

  override def instantiator: Instantiator = CoGroupSubPlanCompiler.CoGroupDriverInstantiator

  override def compile(
    subplan: SubPlan)(
      implicit context: SparkClientCompiler.Context): Type = {
    val subPlanInfo = subplan.getAttribute(classOf[SubPlanInfo])
    val primaryOperator = subPlanInfo.getPrimaryOperator
    assert(primaryOperator.isInstanceOf[UserOperator],
      s"The dominant operator should be user operator: ${primaryOperator}")
    val operator = primaryOperator.asInstanceOf[UserOperator]

    val builder =
      new CoGroupDriverClassBuilder(
        operator)(
        subPlanInfo.getLabel,
        subplan.getOutputs.toSeq)

    context.jpContext.addClass(builder)
  }
}

object CoGroupSubPlanCompiler {

  object CoGroupDriverInstantiator extends Instantiator with NumPartitions {

    override def newInstance(
      driverType: Type,
      subplan: SubPlan)(
        mb: MethodBuilder,
        vars: Instantiator.Vars,
        nextLocal: AtomicInteger)(
          implicit context: SparkClientCompiler.Context): Var = {
      import mb._ // scalastyle:ignore

      val primaryOperator = subplan.getAttribute(classOf[SubPlanInfo]).getPrimaryOperator

      val operatorInfo = new OperatorInfo(primaryOperator)(context.jpContext)
      import operatorInfo._ // scalastyle:ignore

      val properties = inputs.map { input =>
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
            vars.sc.push())(subplan.findInput(inputs.head.getOpposites.head.getOwner))
        })
      val partitionerVar = partitioner.store(nextLocal.getAndAdd(partitioner.size))

      val cogroupDriver = pushNew(driverType)
      cogroupDriver.dup().invokeInit(
        vars.sc.push(),
        vars.hadoopConf.push(),
        vars.broadcasts.push(), {
          // Seq[(Seq[RDD[(K, _)]], Option[Ordering[ShuffleKey]])]
          val builder = getStatic(Seq.getClass.asType, "MODULE$", Seq.getClass.asType)
            .invokeV("newBuilder", classOf[mutable.Builder[_, _]].asType)
          for {
            input <- primaryOperator.getInputs
          } {
            builder.invokeI(NameTransformer.encode("+="),
              classOf[mutable.Builder[_, _]].asType, {
                getStatic(Tuple2.getClass.asType, "MODULE$", Tuple2.getClass.asType)
                  .invokeV("apply", classOf[(_, _)].asType,
                    {
                      val builder = getStatic(Seq.getClass.asType, "MODULE$", Seq.getClass.asType)
                        .invokeV("newBuilder", classOf[mutable.Builder[_, _]].asType)

                      for {
                        opposite <- input.getOpposites.toSet[OperatorOutput]
                        subPlanInput <- Option(subplan.findInput(opposite.getOwner))
                        inputInfo <- Option(subPlanInput.getAttribute(classOf[SubPlanInputInfo]))
                        if inputInfo.getInputType == SubPlanInputInfo.InputType.PARTITIONED
                        prevSubPlanOutput <- subPlanInput.getOpposites.toSeq
                        marker = prevSubPlanOutput.getOperator
                      } {
                        builder.invokeI(
                          NameTransformer.encode("+="),
                          classOf[mutable.Builder[_, _]].asType,
                          vars.rdds.push().invokeI(
                            "apply",
                            classOf[AnyRef].asType,
                            context.branchKeys.getField(mb, marker)
                              .asType(classOf[AnyRef].asType))
                            .cast(classOf[Future[RDD[(ShuffleKey, _)]]].asType)
                            .asType(classOf[AnyRef].asType))
                      }

                      builder
                        .invokeI("result", classOf[AnyRef].asType)
                        .cast(classOf[Seq[_]].asType)
                    }.asType(classOf[AnyRef].asType),
                    {
                      getStatic(Option.getClass.asType, "MODULE$", Option.getClass.asType)
                        .invokeV("apply", classOf[Option[_]].asType, {
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
                        }.asType(classOf[AnyRef].asType))
                    }.asType(classOf[AnyRef].asType)).asType(classOf[AnyRef].asType)
              })
          }
          builder.invokeI("result", classOf[AnyRef].asType).cast(classOf[Seq[_]].asType)
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
}
