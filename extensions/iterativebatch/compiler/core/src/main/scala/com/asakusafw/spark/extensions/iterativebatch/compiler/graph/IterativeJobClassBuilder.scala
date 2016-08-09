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
package com.asakusafw.spark.extensions.iterativebatch.compiler
package graph

import scala.collection.JavaConversions._
import scala.collection.mutable

import org.apache.spark.SparkContext
import org.objectweb.asm.{ Opcodes, Type }
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.lang.compiler.model.graph.MarkerOperator
import com.asakusafw.lang.compiler.planning.{ Plan, Planning, SubPlan }
import com.asakusafw.spark.compiler.graph.Instantiator
import com.asakusafw.spark.compiler.`package`._
import com.asakusafw.spark.compiler.planning.{
  BroadcastInfo,
  IterativeInfo,
  SubPlanInfo,
  SubPlanInputInfo,
  SubPlanOutputInfo
}
import com.asakusafw.spark.compiler.util.SparkIdioms._
import com.asakusafw.spark.runtime.graph.{
  Broadcast,
  BroadcastId,
  Job,
  MapBroadcastOnce,
  Node,
  Source
}
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._
import com.asakusafw.spark.tools.asm4s._
import com.asakusafw.utils.graph.Graphs

import com.asakusafw.spark.extensions.iterativebatch.compiler.spi.RoundAwareNodeCompiler
import com.asakusafw.spark.extensions.iterativebatch.runtime.graph.{
  MapBroadcastAlways,
  MapBroadcastByParameter
}

class IterativeJobClassBuilder(
  plan: Plan)(
    implicit context: IterativeJobCompiler.Context)
  extends ClassBuilder(
    Type.getType(s"L${GeneratedClassPackageInternalName}/${context.flowId}/graph/Job;"),
    classOf[Job].asType) {

  private val subplans = Graphs.sortPostOrder(Planning.toDependencyGraph(plan)).toSeq.zipWithIndex
  private val subplanToIdx = subplans.toMap

  override def defFields(fieldDef: FieldDef): Unit = {
    fieldDef.newField(
      Opcodes.ACC_PRIVATE | Opcodes.ACC_FINAL,
      "sc",
      classOf[SparkContext].asType)
    fieldDef.newField(
      Opcodes.ACC_PRIVATE | Opcodes.ACC_FINAL,
      "nodes",
      classOf[Seq[Node]].asType,
      new TypeSignatureBuilder()
        .newClassType(classOf[Seq[_]].asType) {
          _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[Node].asType)
        })
  }

  override def defConstructors(ctorDef: ConstructorDef): Unit = {
    ctorDef.newInit(Seq(
      classOf[SparkContext].asType)) { implicit mb =>

      val thisVar :: scVar :: _ = mb.argVars

      thisVar.push().invokeInit(
        superType,
        scVar.push())

      thisVar.push().putField("sc", scVar.push())

      val nodesVar = pushNewArray(classOf[Node].asType, subplans.size).store()

      val broadcastsVar = pushObject(mutable.Map)
        .invokeV("empty", classOf[mutable.Map[BroadcastId, Broadcast[_]]].asType)
        .store()

      subplans.foreach {
        case (_, i) =>
          thisVar.push().invokeV(s"node${i}", nodesVar.push(), broadcastsVar.push())
      }

      thisVar.push().putField(
        "nodes", pushObject(Predef)
          .invokeV(
            "wrapRefArray",
            classOf[mutable.WrappedArray[_]].asType,
            nodesVar.push().asType(classOf[Array[AnyRef]].asType))
          .asType(classOf[Seq[_]].asType))
    }
  }

  override def defMethods(methodDef: MethodDef): Unit = {
    super.defMethods(methodDef)

    methodDef.newMethod("nodes", classOf[Seq[Node]].asType, Seq.empty,
      new MethodSignatureBuilder()
        .newReturnType {
          _.newClassType(classOf[Seq[_]].asType) {
            _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[Node].asType)
          }
        }) { implicit mb =>

        val thisVar :: _ = mb.argVars

        `return`(thisVar.push().getField("nodes", classOf[Seq[_]].asType))
      }

    subplans.foreach {
      case (subplan, i) =>
        methodDef.newMethod(
          Opcodes.ACC_PRIVATE,
          s"node${i}",
          Seq(classOf[Array[Node]].asType, classOf[mutable.Map[BroadcastId, Broadcast[_]]].asType))(
            defNodeMethod(subplan, i)(_))
    }
  }

  def defNodeMethod(
    subplan: SubPlan, i: Int)(
      implicit mb: MethodBuilder): Unit = {

    val thisVar :: nodesVar :: allBroadcastsVar :: _ = mb.argVars

    val scVar = thisVar.push().getField("sc", classOf[SparkContext].asType).store()

    val broadcastsVar =
      buildMap { builder =>
        for {
          subPlanInput <- subplan.getInputs
          inputInfo <- Option(subPlanInput.getAttribute(classOf[SubPlanInputInfo]))
          if inputInfo.getInputType == SubPlanInputInfo.InputType.BROADCAST
          broadcastInfo <- Option(subPlanInput.getAttribute(classOf[BroadcastInfo]))
        } {
          val prevSubPlanOutputs = subPlanInput.getOpposites
          if (prevSubPlanOutputs.size == 1) {
            val prevSubPlanOperator = prevSubPlanOutputs.head.getOperator

            builder += (
              context.broadcastIds.getField(subPlanInput.getOperator),
              applyMap(
                allBroadcastsVar.push(),
                context.broadcastIds.getField(prevSubPlanOperator))
              .cast(classOf[Broadcast[_]].asType))
          } else {
            val marker = subPlanInput.getOperator
            val iterativeInfo = IterativeInfo.get(subPlanInput)

            builder += (
              context.broadcastIds.getField(marker),
              newBroadcast(
                marker,
                broadcastInfo,
                iterativeInfo)(
                  () => buildSeq { builder =>
                    prevSubPlanOutputs.foreach { subPlanOutput =>
                      builder += tuple2(
                        nodesVar.push().aload(ldc(subplanToIdx(subPlanOutput.getOwner))),
                        context.branchKeys.getField(subPlanOutput.getOperator))
                    }
                  },
                  scVar.push))
          }
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
      if subPlanOutput.getOpposites.exists(_.getOpposites.size == 1)
    } {
      val marker = subPlanOutput.getOperator
      val iterativeInfo = IterativeInfo.get(subPlanOutput)

      addToMap(
        allBroadcastsVar.push(),
        context.broadcastIds.getField(marker),
        newBroadcast(
          marker,
          broadcastInfo,
          iterativeInfo)(
            () => buildSeq { builder =>
              builder += tuple2(
                nodeVar.push().asType(classOf[Source].asType),
                context.branchKeys.getField(marker))
            },
            scVar.push))
    }
    `return`()
  }

  private def newBroadcast(
    marker: MarkerOperator,
    broadcastInfo: BroadcastInfo,
    iterativeInfo: IterativeInfo)(
      nodes: () => Stack,
      sc: () => Stack)(
        implicit mb: MethodBuilder): Stack = {
    val dataModelRef = marker.getInput.dataModelRef
    val group = broadcastInfo.getFormatInfo
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
    arguments += nodes()
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
    arguments += sc()
    broadcast.invokeInit(arguments.result: _*)
    broadcast
  }
}
