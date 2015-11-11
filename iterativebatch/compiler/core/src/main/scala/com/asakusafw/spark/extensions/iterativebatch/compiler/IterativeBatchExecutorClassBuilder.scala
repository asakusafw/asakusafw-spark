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

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.concurrent.ExecutionContext

import org.apache.spark.SparkContext
import org.objectweb.asm.{ Opcodes, Type }

import com.asakusafw.lang.compiler.planning.{ Plan, Planning }
import com.asakusafw.spark.compiler._
import com.asakusafw.spark.compiler.planning.SubPlanInfo
import com.asakusafw.spark.compiler.planning.{
  BroadcastInfo,
  SubPlanInfo,
  SubPlanInputInfo,
  SubPlanOutputInfo
}
import com.asakusafw.spark.runtime.driver.BroadcastId
import com.asakusafw.spark.tools.asm._
import com.asakusafw.utils.graph.Graphs

import com.asakusafw.spark.extensions.iterativebatch.compiler.flow.Instantiator
import com.asakusafw.spark.extensions.iterativebatch.compiler.spi.NodeCompiler
import com.asakusafw.spark.extensions.iterativebatch.runtime.IterativeBatchExecutor
import com.asakusafw.spark.extensions.iterativebatch.runtime.flow.{
  Broadcast,
  MapBroadcast,
  Node,
  Sink,
  Source,
  Terminator
}

class IterativeBatchExecutorClassBuilder(
  plan: Plan)(
    implicit context: IterativeBatchExecutorCompiler.Context) extends ClassBuilder(
  Type.getType(s"L${GeneratedClassPackageInternalName}/${context.flowId}/IterativeBatchExecutor;"),
  classOf[IterativeBatchExecutor].asType)
  with ScalaIdioms
  with SparkIdioms {

  override def defFields(fieldDef: FieldDef): Unit = {
    fieldDef.newField(
      Opcodes.ACC_PRIVATE,
      "sc",
      classOf[SparkContext].asType)
    fieldDef.newField(
      Opcodes.ACC_PRIVATE,
      "terminator",
      classOf[Terminator].asType)
  }

  override def defConstructors(ctorDef: ConstructorDef): Unit = {
    ctorDef.newInit(Seq(
      classOf[ExecutionContext].asType,
      classOf[SparkContext].asType)) { mb =>
      import mb._ // scalastyle:ignore

      val ecVar = `var`(classOf[ExecutionContext].asType, thisVar.nextLocal)
      val scVar = `var`(classOf[SparkContext].asType, ecVar.nextLocal)

      thisVar.push().invokeInit(ldc(Int.MaxValue), ecVar.push(), scVar.push())
    }

    ctorDef.newInit(Seq(
      Type.INT_TYPE,
      classOf[ExecutionContext].asType,
      classOf[SparkContext].asType)) { mb =>
      import mb._ // scalastyle:ignore

      val numSlotsVar = `var`(Type.INT_TYPE, thisVar.nextLocal)
      val ecVar = `var`(classOf[ExecutionContext].asType, numSlotsVar.nextLocal)
      val scVar = `var`(classOf[SparkContext].asType, ecVar.nextLocal)

      thisVar.push().invokeInit(superType, numSlotsVar.push(), ecVar.push())
      thisVar.push().putField("sc", classOf[SparkContext].asType, scVar.push())
    }
  }

  override def defMethods(methodDef: MethodDef): Unit = {
    super.defMethods(methodDef)

    val subplans = Graphs.sortPostOrder(Planning.toDependencyGraph(plan)).toSeq.zipWithIndex
    val subplanToIdx = subplans.toMap

    methodDef.newMethod(
      "terminator",
      classOf[Terminator].asType,
      Seq.empty) { mb =>
        import mb._ // scalastyle:ignore

        thisVar.push().getField("terminator", classOf[Terminator].asType).unlessNotNull {
          thisVar.push().putField("terminator", classOf[Terminator].asType, {
            val nodesVar = pushNewArray(classOf[Node].asType, ldc(subplans.size))
              .store(thisVar.nextLocal)
            val broadcastsVar = pushObject(mb)(mutable.Map)
              .invokeV("empty", classOf[mutable.Map[BroadcastId, Broadcast]].asType)
              .store(nodesVar.nextLocal)

            subplans.foreach {
              case (_, i) =>
                thisVar.push().invokeV(s"node${i}", nodesVar.push(), broadcastsVar.push())
            }

            val terminator = pushNew(classOf[Terminator].asType)
            terminator.dup().invokeInit(
              buildSeq(mb) { builder =>
                subplans.filter {
                  case (subplan, _) =>
                    subplan.getAttribute(classOf[SubPlanInfo]).getDriverType ==
                      SubPlanInfo.DriverType.OUTPUT
                }.foreach {
                  case (_, i) =>
                    builder +=
                      nodesVar.push().aload(ldc(i)).cast(classOf[Sink].asType)
                }
              },
              thisVar.push().getField("sc", classOf[SparkContext].asType))
            terminator
          })
        }
        `return`(thisVar.push().getField("terminator", classOf[Terminator].asType))
      }

    subplans.foreach {
      case (subplan, i) =>
        val compiler = NodeCompiler.get(subplan)(context.nodeCompilerContext)
        val nodeType = compiler.compile(subplan)(context.nodeCompilerContext)

        methodDef.newMethod(Opcodes.ACC_PRIVATE, s"node${i}", Seq(
          classOf[Array[Node]].asType,
          classOf[mutable.Map[BroadcastId, Broadcast]].asType)) { mb =>
          import mb._ // scalastyle:ignore

          val nodesVar =
            `var`(classOf[Array[Node]].asType, thisVar.nextLocal)
          val allBroadcastsVar =
            `var`(classOf[mutable.Map[BroadcastId, Broadcast]].asType, nodesVar.nextLocal)

          val scVar = thisVar.push().getField("sc", classOf[SparkContext].asType)
            .store(allBroadcastsVar.nextLocal)

          val broadcastsVar =
            buildMap(mb) { builder =>
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
                  context.broadcastIds.getField(mb, subPlanInput.getOperator),
                  applyMap(mb)(
                    allBroadcastsVar.push(),
                    context.broadcastIds.getField(mb, prevSubPlanOperator))
                  .cast(classOf[Broadcast].asType))
              }
            }.store(scVar.nextLocal)

          val nextLocal = new AtomicInteger(broadcastsVar.nextLocal)

          val instantiator = compiler.instantiator
          val nodeVar = instantiator.newInstance(
            nodeType,
            subplan,
            subplanToIdx)(
              mb,
              Instantiator.Vars(scVar, nodesVar, broadcastsVar),
              nextLocal)(
                context.instantiatorCompilerContext)
          nodesVar.push().astore(ldc(i), nodeVar.push())

          for {
            subPlanOutput <- subplan.getOutputs
            outputInfo <- Option(subPlanOutput.getAttribute(classOf[SubPlanOutputInfo]))
            if outputInfo.getOutputType == SubPlanOutputInfo.OutputType.BROADCAST
            broadcastInfo <- Option(subPlanOutput.getAttribute(classOf[BroadcastInfo]))
          } {
            val dataModelRef = subPlanOutput.getOperator.getInput.dataModelRef
            val group = broadcastInfo.getFormatInfo

            addToMap(mb)(
              allBroadcastsVar.push(),
              context.broadcastIds.getField(mb, subPlanOutput.getOperator), {
                // TODO switch broadcast type
                val broadcast = pushNew(classOf[MapBroadcast].asType)
                broadcast.dup().invokeInit(
                  nodeVar.push().asType(classOf[Source].asType),
                  context.branchKeys.getField(mb, subPlanOutput.getOperator),
                  option(mb)(
                    sortOrdering(mb)(
                      dataModelRef.groupingTypes(group.getGrouping),
                      dataModelRef.orderingTypes(group.getOrdering))),
                  groupingOrdering(mb)(dataModelRef.groupingTypes(group.getGrouping)),
                  partitioner(mb)(ldc(1)),
                  ldc(broadcastInfo.getLabel),
                  scVar.push())
                broadcast
              })
          }
          `return`()
        }
    }
  }
}
