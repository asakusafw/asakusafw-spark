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
package com.asakusafw.spark.compiler

import scala.collection.JavaConversions._
import scala.collection.mutable

import org.apache.spark.SparkContext
import org.objectweb.asm.{ Opcodes, Type }

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
import com.asakusafw.spark.compiler.graph.Instantiator
import com.asakusafw.spark.compiler.serializer.{
  BranchKeySerializerClassBuilder,
  BroadcastIdSerializerClassBuilder,
  KryoRegistratorCompiler
}
import com.asakusafw.spark.compiler.spi.NodeCompiler
import com.asakusafw.spark.compiler.util.SparkIdioms._
import com.asakusafw.spark.runtime.DefaultClient
import com.asakusafw.spark.runtime.graph.{
  Broadcast,
  BroadcastId,
  Job,
  MapBroadcastOnce,
  Node,
  Sink,
  Source
}
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._
import com.asakusafw.spark.tools.asm4s._
import com.asakusafw.utils.graph.Graphs

class SparkClientClassBuilder(
  val plan: Plan)(
    implicit context: SparkClientCompiler.Context)
  extends ClassBuilder(
    Type.getType(s"L${GeneratedClassPackageInternalName}/${context.flowId}/SparkClient;"),
    classOf[DefaultClient].asType) {

  override def defMethods(methodDef: MethodDef): Unit = {
    super.defMethods(methodDef)

    val subplans = Graphs.sortPostOrder(Planning.toDependencyGraph(plan)).toSeq.zipWithIndex
    val subplanToIdx = subplans.toMap

    methodDef.newMethod(
      "newJob",
      classOf[Job].asType,
      Seq(classOf[SparkContext].asType)) { implicit mb =>

        val thisVar :: scVar :: _ = mb.argVars

        val nodesVar = pushNewArray(classOf[Node].asType, ldc(subplans.size)).store()
        val broadcastsVar = pushObject(mutable.Map)
          .invokeV("empty", classOf[mutable.Map[BroadcastId, Broadcast]].asType)
          .store()

        subplans.foreach {
          case (_, i) =>
            thisVar.push().invokeV(
              s"node${i}", nodesVar.push(), broadcastsVar.push(), scVar.push())
        }

        val job = pushNew(classOf[Job].asType)
        job.dup().invokeInit(
          pushObject(Predef)
            .invokeV(
              "wrapRefArray",
              classOf[mutable.WrappedArray[_]].asType,
              nodesVar.push().asType(classOf[Array[AnyRef]].asType))
            .asType(classOf[Seq[_]].asType))
        `return`(job)
      }

    subplans.foreach {
      case (subplan, i) =>
        val compiler = NodeCompiler.get(subplan)(context.nodeCompilerContext)
        val nodeType = compiler.compile(subplan)(context.nodeCompilerContext)

        methodDef.newMethod(Opcodes.ACC_PRIVATE, s"node${i}", Seq(
          classOf[Array[Node]].asType,
          classOf[mutable.Map[BroadcastId, Broadcast]].asType,
          classOf[SparkContext].asType)) { implicit mb =>

          val thisVar :: nodesVar :: allBroadcastsVar :: scVar :: _ = mb.argVars

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

            addToMap(
              allBroadcastsVar.push(),
              context.broadcastIds.getField(subPlanOutput.getOperator), {
                val broadcast = pushNew(classOf[MapBroadcastOnce].asType)
                broadcast.dup().invokeInit(
                  nodeVar.push().asType(classOf[Source].asType),
                  context.branchKeys.getField(subPlanOutput.getOperator),
                  option(
                    sortOrdering(
                      dataModelRef.orderingTypes(group.getOrdering))),
                  groupingOrdering,
                  partitioner(ldc(1)),
                  ldc(broadcastInfo.getLabel),
                  scVar.push())
                broadcast
              })
          }
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
