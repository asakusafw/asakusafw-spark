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

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.generic.Growable
import scala.collection.mutable
import scala.collection.JavaConversions._
import scala.concurrent.{ Await, Awaitable, Future }
import scala.concurrent.duration.Duration
import scala.reflect.NameTransformer

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
import com.asakusafw.spark.runtime.driver.{ BroadcastId, ShuffleKey }
import com.asakusafw.spark.runtime.SparkClient
import com.asakusafw.spark.runtime.rdd.BranchKey
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._
import com.asakusafw.utils.graph.Graphs

class SparkClientClassBuilder(
  val plan: Plan)(
    implicit context: SparkClientCompiler.Context)
  extends ClassBuilder(
    Type.getType(s"L${GeneratedClassPackageInternalName}/${context.flowId}/SparkClient;"),
    classOf[SparkClient].asType)
  with ScalaIdioms
  with SparkIdioms {

  override def defFields(fieldDef: FieldDef): Unit = {
    fieldDef.newField("sc", classOf[SparkContext].asType)
    fieldDef.newField("hadoopConf", classOf[Broadcast[Configuration]].asType,
      new TypeSignatureBuilder()
        .newClassType(classOf[Broadcast[_]].asType) {
          _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[Configuration].asType)
        }
        .build())
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
      Seq(classOf[SparkContext].asType, classOf[Broadcast[Configuration]].asType),
      new MethodSignatureBuilder()
        .newParameterType(classOf[SparkContext].asType)
        .newParameterType {
          _.newClassType(classOf[Broadcast[_]].asType) {
            _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[Configuration].asType)
          }
        }
        .newReturnType(Type.INT_TYPE)
        .build()) { mb =>
        import mb._ // scalastyle:ignore
        val scVar = `var`(classOf[SparkContext].asType, thisVar.nextLocal)
        val hadoopConfVar = `var`(classOf[Broadcast[Configuration]].asType, scVar.nextLocal)
        thisVar.push()
          .putField("sc", classOf[SparkContext].asType, scVar.push())
        thisVar.push()
          .putField(
            "hadoopConf",
            classOf[Broadcast[Configuration]].asType,
            hadoopConfVar.push())
        thisVar.push()
          .putField("rdds", classOf[mutable.Map[BranchKey, Future[RDD[_]]]].asType,
            pushObject(mb)(mutable.Map)
              .invokeV("empty", classOf[mutable.Map[BranchKey, Future[RDD[_]]]].asType))
        thisVar.push()
          .putField(
            "broadcasts",
            classOf[mutable.Map[BroadcastId, Future[Broadcast[Map[ShuffleKey, Seq[_]]]]]]
              .asType,
            pushObject(mb)(mutable.Map)
              .invokeV(
                "empty",
                classOf[mutable.Map[BroadcastId, Future[Broadcast[Map[ShuffleKey, Seq[_]]]]]]
                  .asType))
        thisVar.push()
          .putField("terminators", classOf[mutable.Set[Future[Unit]]].asType,
            pushObject(mb)(mutable.Set)
              .invokeV("empty", classOf[mutable.Set[Future[Unit]]].asType))

        subplans.foreach {
          case (_, i) =>
            thisVar.push().invokeV(s"execute${i}")
        }

        val iterVar = thisVar.push()
          .getField("terminators", classOf[mutable.Set[Future[Unit]]].asType)
          .invokeI("iterator", classOf[Iterator[Future[Unit]]].asType)
          .store(hadoopConfVar.nextLocal)
        whileLoop(iterVar.push().invokeI("hasNext", Type.BOOLEAN_TYPE)) { ctrl =>
          pushObject(mb)(Await)
            .invokeV("result", classOf[AnyRef].asType,
              iterVar.push()
                .invokeI("next", classOf[AnyRef].asType)
                .cast(classOf[Future[Unit]].asType)
                .asType(classOf[Awaitable[_]].asType),
              pushObject(mb)(Duration)
                .invokeV("Inf", classOf[Duration.Infinite].asType)
                .asType(classOf[Duration].asType))
            .pop()
        }

        `return`(ldc(0))
      }

    subplans.foreach {
      case (subplan, i) =>
        methodDef.newMethod(s"execute${i}", Seq.empty) { mb =>
          import mb._ // scalastyle:ignore
          val compiler =
            SubPlanCompiler(subplan.getAttribute(classOf[SubPlanInfo]).getDriverType)
          val driverType = compiler.compile(subplan)

          val scVar = thisVar.push()
            .getField("sc", classOf[SparkContext].asType)
            .store(thisVar.nextLocal)
          val hadoopConfVar = thisVar.push()
            .getField("hadoopConf", classOf[Broadcast[Configuration]].asType)
            .store(scVar.nextLocal)
          val rddsVar = thisVar.push()
            .getField("rdds", classOf[mutable.Map[BranchKey, Future[RDD[_]]]].asType)
            .store(hadoopConfVar.nextLocal)
          val terminatorsVar = thisVar.push()
            .getField("terminators", classOf[mutable.Set[Future[Unit]]].asType)
            .store(rddsVar.nextLocal)

          val broadcastsVar =
            buildMap(mb) { builder =>
              for {
                subPlanInput <- subplan.getInputs
                inputInfo <- Option(subPlanInput.getAttribute(classOf[SubPlanInputInfo]))
                if inputInfo.getInputType == SubPlanInputInfo.InputType.BROADCAST
              } {
                val prevSubPlanOutputs = subPlanInput.getOpposites
                assert(prevSubPlanOutputs.size == 1)
                val prevSubPlanOperator = prevSubPlanOutputs.head.getOperator

                builder += (
                  context.broadcastIds.getField(mb, subPlanInput.getOperator),
                  applyMap(mb)(
                    thisVar.push().getField(
                      "broadcasts",
                      classOf[mutable.Map[BroadcastId, Future[Broadcast[Map[ShuffleKey, Seq[_]]]]]]
                        .asType),
                    context.broadcastIds.getField(mb, prevSubPlanOperator))
                  .cast(classOf[Future[Broadcast[Map[ShuffleKey, Seq[_]]]]].asType))
              }
            }.store(terminatorsVar.nextLocal)

          val nextLocal = new AtomicInteger(broadcastsVar.nextLocal)

          val instantiator = compiler.instantiator
          val driverVar = instantiator.newInstance(
            driverType, subplan)(
              mb,
              Instantiator.Vars(scVar, hadoopConfVar, rddsVar, terminatorsVar, broadcastsVar),
              nextLocal)
          val rdds = driverVar.push()
            .invokeV("execute", classOf[Map[BranchKey, Future[RDD[(ShuffleKey, _)]]]].asType)
          val resultVar = rdds.store(nextLocal.getAndAdd(rdds.size))

          for {
            subPlanOutput <- subplan.getOutputs
            outputInfo <- Option(subPlanOutput.getAttribute(classOf[SubPlanOutputInfo]))
            if outputInfo.getOutputType == SubPlanOutputInfo.OutputType.BROADCAST
            broadcastInfo <- Option(subPlanOutput.getAttribute(classOf[BroadcastInfo]))
          } {
            val dataModelRef = subPlanOutput.getOperator.getInput.dataModelRef
            val group = broadcastInfo.getFormatInfo

            thisVar.push().getField(
              "broadcasts",
              classOf[mutable.Map[BroadcastId, Future[Broadcast[Map[ShuffleKey, Seq[_]]]]]]
                .asType)
              .invokeI(
                NameTransformer.encode("+="),
                classOf[Growable[_]].asType,
                tuple2(mb)(
                  context.broadcastIds.getField(mb, subPlanOutput.getOperator),
                  thisVar.push().invokeV(
                    "broadcastAsHash",
                    classOf[Future[Broadcast[_]]].asType,
                    scVar.push(),
                    ldc(broadcastInfo.getLabel),
                    applyMap(mb)(
                      resultVar.push(),
                      context.branchKeys.getField(mb, subPlanOutput.getOperator))
                      .cast(classOf[Future[RDD[(ShuffleKey, _)]]].asType),
                    option(mb)(
                      sortOrdering(mb)(
                        dataModelRef.groupingTypes(group.getGrouping),
                        dataModelRef.orderingTypes(group.getOrdering))),
                    groupingOrdering(mb)(dataModelRef.groupingTypes(group.getGrouping)),
                    partitioner(mb)(ldc(1))))
                  .asType(classOf[AnyRef].asType))
              .pop()
          }

          rddsVar.push().invokeI(
            NameTransformer.encode("++="),
            classOf[Growable[_]].asType,
            resultVar.push().asType(classOf[TraversableOnce[_]].asType))
            .pop()

          `return`()
        }
    }

    val branchKeysType = context.jpContext.addClass(context.branchKeys)
    val broadcastIdsType = context.jpContext.addClass(context.broadcastIds)

    val registrator = KryoRegistratorCompiler.compile(
      OperatorUtil.collectDataTypes(
        plan.getElements.toSet[SubPlan].flatMap(_.getOperators.toSet[Operator]))
        .toSet[TypeDescription]
        .map(_.asType),
      context.jpContext.addClass(new BranchKeySerializerClassBuilder(branchKeysType)),
      context.jpContext.addClass(new BroadcastIdSerializerClassBuilder(broadcastIdsType)))

    methodDef.newMethod("kryoRegistrator", classOf[String].asType, Seq.empty) { mb =>
      import mb._ // scalastyle:ignore
      `return`(ldc(registrator.getClassName))
    }
  }
}
