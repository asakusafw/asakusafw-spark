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

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.concurrent.ExecutionContext

import org.apache.spark.SparkContext
import org.objectweb.asm.{ Opcodes, Type }

import com.asakusafw.lang.compiler.planning.{ SubPlan, Plan, Planning }
import com.asakusafw.spark.compiler.`package`._
import com.asakusafw.spark.compiler.planning.SubPlanInfo
import com.asakusafw.spark.compiler.planning.{
  BroadcastInfo,
  IterativeInfo,
  SubPlanInputInfo,
  SubPlanOutputInfo
}
import com.asakusafw.spark.compiler.util.SparkIdioms._
import com.asakusafw.spark.runtime.graph.{ BroadcastId, MapBroadcastOnce }
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._
import com.asakusafw.spark.tools.asm4s._
import com.asakusafw.utils.graph.Graphs

import com.asakusafw.spark.compiler.graph.Instantiator
import com.asakusafw.spark.runtime.graph.{
  Broadcast,
  Job,
  Node,
  Sink,
  Source
}

import com.asakusafw.spark.extensions.iterativebatch.compiler.spi.RoundAwareNodeCompiler
import com.asakusafw.spark.extensions.iterativebatch.runtime.IterativeBatchExecutor
import com.asakusafw.spark.extensions.iterativebatch.runtime.graph.{
  MapBroadcastAlways,
  MapBroadcastByParameter
}

class IterativeBatchExecutorClassBuilder(
  plan: Plan)(
    implicit context: IterativeBatchExecutorCompiler.Context) extends ClassBuilder(
  Type.getType(s"L${GeneratedClassPackageInternalName}/${context.flowId}/IterativeBatchExecutor;"),
  classOf[IterativeBatchExecutor].asType) {

  override def defFields(fieldDef: FieldDef): Unit = {
    fieldDef.newField(
      Opcodes.ACC_PRIVATE,
      "sc",
      classOf[SparkContext].asType)
    fieldDef.newField(
      Opcodes.ACC_PRIVATE,
      "job",
      classOf[Job]asType)
  }

  override def defConstructors(ctorDef: ConstructorDef): Unit = {
    ctorDef.newInit(Seq(
      classOf[ExecutionContext].asType,
      classOf[SparkContext].asType)) { implicit mb =>

      val thisVar :: ecVar :: scVar :: _ = mb.argVars

      thisVar.push().invokeInit(ldc(Int.MaxValue), ecVar.push(), scVar.push())
    }

    ctorDef.newInit(Seq(
      Type.INT_TYPE,
      classOf[ExecutionContext].asType,
      classOf[SparkContext].asType)) { implicit mb =>

      val thisVar :: numSlotsVar :: ecVar :: scVar :: _ = mb.argVars

      thisVar.push().invokeInit(superType, numSlotsVar.push(), ecVar.push())
      thisVar.push().putField("sc", scVar.push())
    }
  }

  override def defMethods(methodDef: MethodDef): Unit = {
    super.defMethods(methodDef)

    val subplans = Graphs.sortPostOrder(Planning.toDependencyGraph(plan)).toSeq.zipWithIndex
    val subplanToIdx = subplans.toMap

    methodDef.newMethod(
      "job",
      classOf[Job].asType,
      Seq.empty) { implicit mb =>

        val thisVar :: _ = mb.argVars

        thisVar.push().getField("job", classOf[Job].asType).unlessNotNull {
          thisVar.push().putField("job", {
            val nodesVar = pushNewArray(classOf[Node].asType, ldc(subplans.size)).store()
            val broadcastsVar = pushObject(mutable.Map)
              .invokeV("empty", classOf[mutable.Map[BroadcastId, Broadcast]].asType)
              .store()

            subplans.foreach {
              case (_, i) =>
                thisVar.push().invokeV(s"node${i}", nodesVar.push(), broadcastsVar.push())
            }

            val job = pushNew(classOf[Job].asType)
            job.dup().invokeInit(
              pushObject(Predef)
                .invokeV(
                  "wrapRefArray",
                  classOf[mutable.WrappedArray[_]].asType,
                  nodesVar.push().asType(classOf[Array[AnyRef]].asType))
                .asType(classOf[Seq[_]].asType))
            job
          })
        }
        `return`(thisVar.push().getField("job", classOf[Job].asType))
      }

    subplans.foreach {
      case (subplan, i) =>
        methodDef.newMethod(Opcodes.ACC_PRIVATE, s"node${i}", Seq(
          classOf[Array[Node]].asType,
          classOf[mutable.Map[BroadcastId, Broadcast]].asType))(
          defNodeMethod(subplan, i)(subplanToIdx)(_))
    }
  }

  def defNodeMethod(
    subplan: SubPlan, i: Int)(
      subplanToIdx: Map[SubPlan, Int])(
        implicit mb: MethodBuilder): Unit = {

    val thisVar :: nodesVar :: allBroadcastsVar :: _ = mb.argVars

    val scVar = thisVar.push().getField("sc", classOf[SparkContext].asType).store()

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
              allBroadcastsVar.push(),
              context.broadcastIds.getField(prevSubPlanOperator))
            .cast(classOf[Broadcast].asType))
        }
      }.store()

    val compiler = RoundAwareNodeCompiler.get(subplan)(context.nodeCompilerContext)
    val nodeType = compiler.compile(subplan)(context.nodeCompilerContext)

    val instantiator = compiler.instantiator
    val nodeVar = instantiator.newInstance(
      nodeType,
      subplan,
      subplanToIdx)(
        Instantiator.Vars(scVar, nodesVar, broadcastsVar))(
          implicitly, context.instantiatorCompilerContext)
    nodesVar.push().astore(ldc(i), nodeVar.push())

    for {
      subPlanOutput <- subplan.getOutputs
      outputInfo <- Option(subPlanOutput.getAttribute(classOf[SubPlanOutputInfo]))
      if outputInfo.getOutputType == SubPlanOutputInfo.OutputType.BROADCAST
      broadcastInfo <- Option(subPlanOutput.getAttribute(classOf[BroadcastInfo]))
    } {
      val dataModelRef = subPlanOutput.getOperator.getInput.dataModelRef
      val group = broadcastInfo.getFormatInfo

      val iterativeInfo = IterativeInfo.get(subPlanOutput)

      addToMap(
        allBroadcastsVar.push(),
        context.broadcastIds.getField(subPlanOutput.getOperator), {
          val broadcast = iterativeInfo.getRecomputeKind match {
            case IterativeInfo.RecomputeKind.ALWAYS =>
              pushNew(classOf[MapBroadcastAlways].asType)
            case IterativeInfo.RecomputeKind.PARAMETER =>
              pushNew(classOf[MapBroadcastByParameter].asType)
            case IterativeInfo.RecomputeKind.NEVER =>
              pushNew(classOf[MapBroadcastOnce].asType)
          }
          broadcast.dup()
          val arguments = Seq.newBuilder[Stack]
          arguments += nodeVar.push().asType(classOf[Source].asType)
          arguments += context.branchKeys.getField(subPlanOutput.getOperator)
          arguments += option(
            sortOrdering(
              dataModelRef.groupingTypes(group.getGrouping),
              dataModelRef.orderingTypes(group.getOrdering)))
          arguments += groupingOrdering(dataModelRef.groupingTypes(group.getGrouping))
          arguments += partitioner(ldc(1))
          arguments += ldc(broadcastInfo.getLabel)
          if (iterativeInfo.getRecomputeKind == IterativeInfo.RecomputeKind.PARAMETER) {
            arguments +=
              buildSet { builder =>
                iterativeInfo.getParameters.foreach { parameter =>
                  builder += ldc(parameter)
                }
              }
          }
          arguments += scVar.push()
          broadcast.invokeInit(arguments.result: _*)
          broadcast
        })
    }
    `return`()
  }
}
