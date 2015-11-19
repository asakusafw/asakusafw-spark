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

import scala.collection.mutable
import scala.collection.JavaConversions._
import scala.concurrent.{ Await, Awaitable, ExecutionContext, Future }
import scala.concurrent.duration.Duration

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.objectweb.asm.Type
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.lang.compiler.analyzer.util.OperatorUtil
import com.asakusafw.lang.compiler.model.description.TypeDescription
import com.asakusafw.lang.compiler.model.graph.Operator
import com.asakusafw.lang.compiler.planning.{ Plan, Planning, SubPlan }
import com.asakusafw.spark.compiler.planning.{
  BroadcastInfo,
  SubPlanInfo,
  SubPlanInputInfo,
  SubPlanOutputInfo
}
import com.asakusafw.spark.compiler.serializer.{
  BranchKeySerializerClassBuilder,
  BroadcastIdSerializerClassBuilder,
  KryoRegistratorCompiler
}
import com.asakusafw.spark.compiler.spi.SubPlanCompiler
import com.asakusafw.spark.compiler.subplan.Instantiator
import com.asakusafw.spark.compiler.util.SparkIdioms._
import com.asakusafw.spark.runtime.driver.{ BroadcastId, ShuffleKey }
import com.asakusafw.spark.runtime.SparkClient
import com.asakusafw.spark.runtime.rdd.BranchKey
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._
import com.asakusafw.spark.tools.asm4s._
import com.asakusafw.utils.graph.Graphs

class SparkClientClassBuilder(
  val plan: Plan)(
    implicit context: SparkClientCompiler.Context)
  extends ClassBuilder(
    Type.getType(s"L${GeneratedClassPackageInternalName}/${context.flowId}/SparkClient;"),
    classOf[SparkClient].asType) {

  override def defFields(fieldDef: FieldDef): Unit = {
    fieldDef.newField("sc", classOf[SparkContext].asType)
    fieldDef.newField("hadoopConf", classOf[Broadcast[Configuration]].asType,
      new TypeSignatureBuilder()
        .newClassType(classOf[Broadcast[_]].asType) {
          _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[Configuration].asType)
        }
        .build())
    fieldDef.newField("ec", classOf[ExecutionContext].asType)
    fieldDef.newField("rdds", classOf[mutable.Map[BranchKey, Future[RDD[_]]]].asType,
      new TypeSignatureBuilder()
        .newClassType(classOf[mutable.Map[_, _]].asType) {
          _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[BranchKey].asType)
            .newTypeArgument(SignatureVisitor.INSTANCEOF) {
              _.newClassType(classOf[Future[_]].asType) {
                _.newTypeArgument(SignatureVisitor.INSTANCEOF) {
                  _.newClassType(classOf[RDD[_]].asType) {
                    _.newTypeArgument()
                  }
                }
              }
            }
        }
        .build())
    fieldDef.newField(
      "broadcasts",
      classOf[mutable.Map[BroadcastId, Future[Broadcast[Map[ShuffleKey, Seq[_]]]]]].asType,
      new TypeSignatureBuilder()
        .newClassType(classOf[mutable.Map[_, _]].asType) {
          _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[BroadcastId].asType)
            .newTypeArgument(SignatureVisitor.INSTANCEOF) {
              _.newClassType(classOf[Future[_]].asType) {
                _.newTypeArgument(SignatureVisitor.INSTANCEOF) {
                  _.newClassType(classOf[Broadcast[_]].asType) {
                    _.newTypeArgument(SignatureVisitor.INSTANCEOF) {
                      _.newClassType(classOf[Map[_, _]].asType) {
                        _.newTypeArgument(
                          SignatureVisitor.INSTANCEOF, classOf[ShuffleKey].asType)
                          .newTypeArgument(SignatureVisitor.INSTANCEOF) {
                            _.newClassType(classOf[Seq[_]].asType) {
                              _.newTypeArgument()
                            }
                          }
                      }
                    }
                  }
                }
              }
            }
        }
        .build())
    fieldDef.newField("terminators", classOf[mutable.Set[Future[Unit]]].asType,
      new TypeSignatureBuilder()
        .newClassType(classOf[mutable.Set[Future[Unit]]].asType) {
          _.newTypeArgument(SignatureVisitor.INSTANCEOF) {
            _.newClassType(classOf[Future[Unit]].asType) {
              _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[Unit].asType)
            }
          }
        }
        .build())
  }

  override def defMethods(methodDef: MethodDef): Unit = {
    val subplans = Graphs.sortPostOrder(Planning.toDependencyGraph(plan)).toSeq.zipWithIndex

    methodDef.newMethod(
      "execute",
      Type.INT_TYPE,
      Seq(
        classOf[SparkContext].asType,
        classOf[Broadcast[Configuration]].asType,
        classOf[ExecutionContext].asType),
      new MethodSignatureBuilder()
        .newParameterType(classOf[SparkContext].asType)
        .newParameterType {
          _.newClassType(classOf[Broadcast[_]].asType) {
            _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[Configuration].asType)
          }
        }
        .newParameterType(classOf[ExecutionContext].asType)
        .newReturnType(Type.INT_TYPE)
        .build()) { implicit mb =>
        val thisVar :: scVar :: hadoopConfVar :: ecVar :: _ = mb.argVars
        thisVar.push()
          .putField("sc", classOf[SparkContext].asType, scVar.push())
        thisVar.push()
          .putField(
            "hadoopConf",
            classOf[Broadcast[Configuration]].asType,
            hadoopConfVar.push())
        thisVar.push()
          .putField("ec", classOf[ExecutionContext].asType, ecVar.push())
        thisVar.push()
          .putField("rdds", classOf[mutable.Map[BranchKey, Future[RDD[_]]]].asType,
            pushObject(mutable.Map)
              .invokeV("empty", classOf[mutable.Map[BranchKey, Future[RDD[_]]]].asType))
        thisVar.push()
          .putField(
            "broadcasts",
            classOf[mutable.Map[BroadcastId, Future[Broadcast[Map[ShuffleKey, Seq[_]]]]]]
              .asType,
            pushObject(mutable.Map)
              .invokeV(
                "empty",
                classOf[mutable.Map[BroadcastId, Future[Broadcast[Map[ShuffleKey, Seq[_]]]]]]
                  .asType))
        thisVar.push()
          .putField("terminators", classOf[mutable.Set[Future[Unit]]].asType,
            pushObject(mutable.Set)
              .invokeV("empty", classOf[mutable.Set[Future[Unit]]].asType))

        subplans.foreach {
          case (_, i) =>
            thisVar.push().invokeV(s"execute${i}")
        }

        val iterVar = thisVar.push()
          .getField("terminators", classOf[mutable.Set[Future[Unit]]].asType)
          .invokeI("iterator", classOf[Iterator[Future[Unit]]].asType)
          .store()
        whileLoop(iterVar.push().invokeI("hasNext", Type.BOOLEAN_TYPE)) { ctrl =>
          pushObject(Await)
            .invokeV("result", classOf[AnyRef].asType,
              iterVar.push()
                .invokeI("next", classOf[AnyRef].asType)
                .cast(classOf[Future[Unit]].asType)
                .asType(classOf[Awaitable[_]].asType),
              pushObject(Duration)
                .invokeV("Inf", classOf[Duration.Infinite].asType)
                .asType(classOf[Duration].asType))
            .pop()
        }

        `return`(ldc(0))
      }

    subplans.foreach {
      case (subplan, i) =>
        methodDef.newMethod(s"execute${i}", Seq.empty) { implicit mb =>
          val thisVar :: _ = mb.argVars

          val compiler =
            SubPlanCompiler(subplan.getAttribute(classOf[SubPlanInfo]).getDriverType)(
              context.subplanCompilerContext)
          val driverType = compiler.compile(subplan)(context.subplanCompilerContext)

          val scVar = thisVar.push()
            .getField("sc", classOf[SparkContext].asType)
            .store()
          val hadoopConfVar = thisVar.push()
            .getField("hadoopConf", classOf[Broadcast[Configuration]].asType)
            .store()
          val ecVar = thisVar.push()
            .getField("ec", classOf[ExecutionContext].asType)
            .store()
          val rddsVar = thisVar.push()
            .getField("rdds", classOf[mutable.Map[BranchKey, Future[RDD[_]]]].asType)
            .store()
          val terminatorsVar = thisVar.push()
            .getField("terminators", classOf[mutable.Set[Future[Unit]]].asType)
            .store()

          val broadcastsVar =
            buildMap { builder =>
              for {
                subPlanInput <- subplan.getInputs
                inputInfo <- Option(subPlanInput.getAttribute(classOf[SubPlanInputInfo]))
                if inputInfo.getInputType == SubPlanInputInfo.InputType.BROADCAST
              } {
                val prevSubPlanOutputs = subPlanInput.getOpposites
                assert(prevSubPlanOutputs.size == 1,
                  s"The number of input for broadcast should be 1: ${prevSubPlanOutputs.size}")
                val prevSubPlanOperator = prevSubPlanOutputs.head.getOperator

                builder += (
                  context.broadcastIds.getField(subPlanInput.getOperator),
                  applyMap(
                    thisVar.push().getField(
                      "broadcasts",
                      classOf[mutable.Map[BroadcastId, Future[Broadcast[Map[ShuffleKey, Seq[_]]]]]]
                        .asType),
                    context.broadcastIds.getField(prevSubPlanOperator))
                  .cast(classOf[Future[Broadcast[Map[ShuffleKey, Seq[_]]]]].asType))
              }
            }.store()

          val instantiator = compiler.instantiator
          val driverVar = instantiator.newInstance(
            driverType,
            subplan)(
              Instantiator.Vars(scVar, hadoopConfVar, rddsVar, terminatorsVar, broadcastsVar))(
                implicitly, context.instantiatorCompilerContext)
          val resultVar = driverVar.push()
            .invokeV(
              "execute",
              classOf[Map[BranchKey, Future[RDD[(ShuffleKey, _)]]]].asType,
              ecVar.push())
            .store()

          for {
            subPlanOutput <- subplan.getOutputs
            outputInfo <- Option(subPlanOutput.getAttribute(classOf[SubPlanOutputInfo]))
            if outputInfo.getOutputType == SubPlanOutputInfo.OutputType.BROADCAST
            broadcastInfo <- Option(subPlanOutput.getAttribute(classOf[BroadcastInfo]))
          } {
            val dataModelRef = subPlanOutput.getOperator.getInput.dataModelRef
            val group = broadcastInfo.getFormatInfo

            addToMap(
              thisVar.push().getField(
                "broadcasts",
                classOf[mutable.Map[BroadcastId, Future[Broadcast[Map[ShuffleKey, Seq[_]]]]]]
                  .asType),
              context.broadcastIds.getField(subPlanOutput.getOperator),
              thisVar.push().invokeV(
                "broadcastAsHash",
                classOf[Future[Broadcast[_]]].asType,
                scVar.push(),
                ldc(broadcastInfo.getLabel),
                applyMap(
                  resultVar.push(),
                  context.branchKeys.getField(subPlanOutput.getOperator))
                  .cast(classOf[Future[RDD[(ShuffleKey, _)]]].asType),
                option(
                  sortOrdering(
                    dataModelRef.groupingTypes(group.getGrouping),
                    dataModelRef.orderingTypes(group.getOrdering))),
                groupingOrdering(dataModelRef.groupingTypes(group.getGrouping)),
                partitioner(ldc(1)),
                ecVar.push()))
          }

          addTraversableToMap(rddsVar.push(), resultVar.push())

          `return`()
        }
    }

    val branchKeysType = context.addClass(context.branchKeys)
    val broadcastIdsType = context.addClass(context.broadcastIds)

    val registrator = KryoRegistratorCompiler.compile(
      OperatorUtil.collectDataTypes(
        plan.getElements.toSet[SubPlan].flatMap(_.getOperators.toSet[Operator]))
        .toSet[TypeDescription]
        .map(_.asType),
      context.addClass(new BranchKeySerializerClassBuilder(branchKeysType)),
      context.addClass(new BroadcastIdSerializerClassBuilder(broadcastIdsType)))

    methodDef.newMethod("kryoRegistrator", classOf[String].asType, Seq.empty) { implicit mb =>
      `return`(ldc(registrator.getClassName))
    }
  }
}
