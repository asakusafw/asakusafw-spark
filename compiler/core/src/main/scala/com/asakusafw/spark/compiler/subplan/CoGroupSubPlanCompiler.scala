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
import scala.reflect.{ ClassTag, NameTransformer }

import org.apache.spark.{ HashPartitioner, Partitioner, SparkContext }
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.objectweb.asm.Type
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.lang.compiler.model.graph._
import com.asakusafw.lang.compiler.planning.{ PlanMarker, SubPlan }
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.spark.compiler.operator._
import com.asakusafw.spark.compiler.ordering.{ GroupingOrderingClassBuilder, SortOrderingClassBuilder }
import com.asakusafw.spark.compiler.planning.{ SubPlanInfo, SubPlanInputInfo }
import com.asakusafw.spark.compiler.spi.{ OperatorCompiler, OperatorType, SubPlanCompiler }
import com.asakusafw.spark.runtime.driver.{ BroadcastId, ShuffleKey }
import com.asakusafw.spark.runtime.fragment._
import com.asakusafw.spark.runtime.rdd.BranchKey
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._

class CoGroupSubPlanCompiler extends SubPlanCompiler {

  import CoGroupSubPlanCompiler._

  def of: SubPlanInfo.DriverType = SubPlanInfo.DriverType.COGROUP

  override def instantiator: Instantiator = CoGroupSubPlanCompiler.CoGroupDriverInstantiator

  override def compile(subplan: SubPlan)(implicit context: Context): Type = {
    val subPlanInfo = subplan.getAttribute(classOf[SubPlanInfo])
    val primaryOperator = subPlanInfo.getPrimaryOperator
    assert(primaryOperator.isInstanceOf[UserOperator],
      s"The dominant operator should be user operator: ${primaryOperator}")
    val operator = primaryOperator.asInstanceOf[UserOperator]

    val builder = new CoGroupDriverClassBuilder(context.flowId) {

      override val jpContext = context.jpContext

      override val branchKeys: BranchKeys = context.branchKeys

      override val label: String = subPlanInfo.getLabel

      override val subplanOutputs: Seq[SubPlan.Output] = subplan.getOutputs.toSeq

      override def defMethods(methodDef: MethodDef): Unit = {
        super.defMethods(methodDef)

        methodDef.newMethod("fragments", classOf[(_, _)].asType, Seq(classOf[Map[BroadcastId, Broadcast[_]]].asType),
          new MethodSignatureBuilder()
            .newParameterType {
              _.newClassType(classOf[Map[_, _]].asType) {
                _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[BroadcastId].asType)
                  .newTypeArgument(SignatureVisitor.INSTANCEOF) {
                    _.newClassType(classOf[Broadcast[_]].asType) {
                      _.newTypeArgument()
                    }
                  }
              }
            }
            .newReturnType {
              _.newClassType(classOf[(_, _)].asType) {
                _.newTypeArgument(SignatureVisitor.INSTANCEOF) {
                  _.newClassType(classOf[Fragment[_]].asType) {
                    _.newTypeArgument(SignatureVisitor.INSTANCEOF) {
                      _.newClassType(classOf[Seq[_]].asType) {
                        _.newTypeArgument(SignatureVisitor.INSTANCEOF) {
                          _.newClassType(classOf[Iterable[_]].asType) {
                            _.newTypeArgument()
                          }
                        }
                      }
                    }
                  }
                }
                  .newTypeArgument(SignatureVisitor.INSTANCEOF) {
                    _.newClassType(classOf[Map[_, _]].asType) {
                      _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[BranchKey].asType)
                        .newTypeArgument(SignatureVisitor.INSTANCEOF) {
                          _.newClassType(classOf[OutputFragment[_]].asType) {
                            _.newTypeArgument()
                          }
                        }
                    }
                  }
              }
            }
            .build()) { mb =>
            import mb._
            val broadcastsVar = `var`(classOf[Map[BroadcastId, Broadcast[_]]].asType, thisVar.nextLocal)
            val nextLocal = new AtomicInteger(broadcastsVar.nextLocal)

            implicit val compilerContext =
              OperatorCompiler.Context(
                flowId = context.flowId,
                jpContext = context.jpContext,
                branchKeys = context.branchKeys,
                broadcastIds = context.broadcastIds)
            val fragmentBuilder = new FragmentTreeBuilder(mb, broadcastsVar, nextLocal)
            val fragmentVar = {
              val t = OperatorCompiler.compile(operator, OperatorType.CoGroupType)
              val outputs = operator.getOutputs.map(fragmentBuilder.build)
              val fragment = pushNew(t)
              fragment.dup().invokeInit(
                broadcastsVar.push()
                  +: outputs.map(_.push().asType(classOf[Fragment[_]].asType)): _*)
              fragment.store(nextLocal.getAndAdd(fragment.size))
            }
            val outputsVar = fragmentBuilder.buildOutputsVar(subplanOutputs)

            `return`(
              getStatic(Tuple2.getClass.asType, "MODULE$", Tuple2.getClass.asType).
                invokeV("apply", classOf[(_, _)].asType,
                  fragmentVar.push().asType(classOf[AnyRef].asType), outputsVar.push().asType(classOf[AnyRef].asType)))
          }
      }
    }

    context.jpContext.addClass(builder)
  }
}

object CoGroupSubPlanCompiler {

  object CoGroupDriverInstantiator extends Instantiator with NumPartitions {

    override def newInstance(
      driverType: Type,
      subplan: SubPlan)(implicit context: Context): Var = {
      import context.mb._

      val primaryOperator = subplan.getAttribute(classOf[SubPlanInfo]).getPrimaryOperator

      val operatorInfo = new OperatorInfo(primaryOperator)(context.jpContext)
      import operatorInfo._

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
          numPartitions(context.mb, context.scVar.push())(subplan.findInput(inputs.head.getOpposites.head.getOwner))
        })
      val partitionerVar = partitioner.store(context.nextLocal.getAndAdd(partitioner.size))

      val cogroupDriver = pushNew(driverType)
      cogroupDriver.dup().invokeInit(
        context.scVar.push(),
        context.hadoopConfVar.push(),
        context.broadcastsVar.push(), {
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
                        builder.invokeI(NameTransformer.encode("+="), classOf[mutable.Builder[_, _]].asType,
                          context.rddsVar.push().invokeI(
                            "apply",
                            classOf[AnyRef].asType,
                            context.branchKeys.getField(context.mb, marker)
                              .asType(classOf[AnyRef].asType))
                            .cast(classOf[Future[RDD[(ShuffleKey, _)]]].asType)
                            .asType(classOf[AnyRef].asType))
                      }

                      builder.invokeI("result", classOf[AnyRef].asType).cast(classOf[Seq[_]].asType)
                    }.asType(classOf[AnyRef].asType),
                    {
                      getStatic(Option.getClass.asType, "MODULE$", Option.getClass.asType)
                        .invokeV("apply", classOf[Option[_]].asType, {
                          val dataModelRef = context.jpContext.getDataModelLoader.load(input.getDataType)
                          pushNew0(
                            SortOrderingClassBuilder.getOrCompile(
                              context.flowId,
                              input.getGroup.getGrouping.map { grouping =>
                                dataModelRef.findProperty(grouping).getType.asType
                              },
                              input.getGroup.getOrdering.map { ordering =>
                                (dataModelRef.findProperty(ordering.getPropertyName).getType.asType,
                                  ordering.getDirection == Group.Direction.ASCENDANT)
                              },
                              context.jpContext))
                        }.asType(classOf[AnyRef].asType))
                    }.asType(classOf[AnyRef].asType)).asType(classOf[AnyRef].asType)
              })
          }
          builder.invokeI("result", classOf[AnyRef].asType).cast(classOf[Seq[_]].asType)
        }, {
          // ShuffleKey.GroupingOrdering
          pushNew0(GroupingOrderingClassBuilder.getOrCompile(context.flowId, properties.head, context.jpContext))
            .asType(classOf[Ordering[ShuffleKey]].asType)
        }, {
          // Partitioner
          partitionerVar.push().asType(classOf[Partitioner].asType)
        })
      cogroupDriver.store(context.nextLocal.getAndAdd(cogroupDriver.size))
    }
  }
}
