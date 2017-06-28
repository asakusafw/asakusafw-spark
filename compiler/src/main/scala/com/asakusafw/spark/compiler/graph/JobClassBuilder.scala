/*
 * Copyright 2011-2017 Asakusa Framework Team.
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
package graph

import scala.collection.JavaConversions._
import scala.collection.mutable
import org.objectweb.asm.{ Opcodes, Type }
import org.objectweb.asm.signature.SignatureVisitor
import com.asakusafw.lang.compiler.extension.directio.DirectFileIoModels
import com.asakusafw.lang.compiler.model.graph.{ ExternalOutput, MarkerOperator }
import com.asakusafw.lang.compiler.planning.{ Plan, Planning, SubPlan }
import com.asakusafw.spark.compiler.planning._
import com.asakusafw.spark.compiler.spi.NodeCompiler
import com.asakusafw.spark.compiler.util.SparkIdioms._
import com.asakusafw.spark.runtime.JobContext
import com.asakusafw.spark.runtime.graph._
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._
import com.asakusafw.spark.tools.asm4s._
import com.asakusafw.utils.graph.Graphs

class JobClassBuilder(
  val plan: Plan)(
    implicit context: JobCompiler.Context)
  extends ClassBuilder(
    Type.getType(s"L${GeneratedClassPackageInternalName}/${context.flowId}/graph/Job;"),
    classOf[Job].asType) {

  private val directOutputs = collectDirectOutputs(plan.getElements.toSet[SubPlan])

  private def useDirectOut: Boolean = directOutputs.nonEmpty

  private val subplans = Graphs.sortPostOrder(Planning.toDependencyGraph(plan)).toSeq
  private val subplanToIdx = if (useDirectOut) {
    subplans.zipWithIndex.map {
      case (subplan, i) => subplan -> (i + 1)
    }.toMap
  } else {
    subplans.zipWithIndex.toMap
  }

  override def defFields(fieldDef: FieldDef): Unit = {
    super.defFields(fieldDef)

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

    ctorDef.newInit(Seq(classOf[JobContext].asType)) { implicit mb =>

      val thisVar :: jobContextVar :: _ = mb.argVars

      thisVar.push().invokeInit(
        superType,
        jobContextVar.push())

      val nodesVar = pushNewArray(
        classOf[Node].asType,
        if (useDirectOut) subplans.size + 2 else subplans.size).store()

      val broadcastsVar = pushObject(mutable.Map)
        .invokeV("empty", classOf[mutable.Map[BroadcastId, Broadcast[_]]].asType)
        .store()

      if (useDirectOut) {
        thisVar.push().invokeV("node0", nodesVar.push(), jobContextVar.push())
      }

      subplans.foreach { subplan =>
        thisVar.push().invokeV(
          s"node${subplanToIdx(subplan)}",
          nodesVar.push(),
          broadcastsVar.push(),
          jobContextVar.push())
      }

      if (useDirectOut) {
        thisVar.push().invokeV(s"node${subplans.size + 1}", nodesVar.push(), jobContextVar.push())
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

    if (useDirectOut) {
      defSetupNode(methodDef)
    }

    subplans.foreach { subplan =>
      defNode(methodDef)(subplan)
    }

    if (useDirectOut) {
      defCommitNode(methodDef)
    }
  }

  private def collectDirectOutputs(subplans: Set[SubPlan]): Set[(SubPlan, ExternalOutput)] = {
    if (context.options.useOutputDirect) {
      for {
        subplan <- subplans
        subPlanInfo = subplan.getAttribute(classOf[SubPlanInfo])
        primaryOperator = subPlanInfo.getPrimaryOperator
        if primaryOperator.isInstanceOf[ExternalOutput]
        operator = primaryOperator.asInstanceOf[ExternalOutput]
        info <- Option(operator.getInfo)
        if DirectFileIoModels.isSupported(info)
      } yield {
        subplan -> operator
      }
    } else {
      Set.empty
    }
  }

  private def defSetupNode(methodDef: MethodDef): Unit = {
    methodDef.newMethod(
      Opcodes.ACC_PRIVATE,
      "node0",
      Seq(classOf[Array[Node]].asType, classOf[JobContext].asType)) { implicit mb =>

        val thisVar :: nodesVar :: jobContextVar :: _ = mb.argVars

        nodesVar.push().astore(ldc(0), {
          val setupType = DirectOutputSetupCompiler.compile(directOutputs.map(_._2))
          val setup = pushNew(setupType)
          setup.dup().invokeInit(jobContextVar.push())
          setup
        })
        `return`()
      }
  }

  private def defCommitNode(methodDef: MethodDef): Unit = {
    methodDef.newMethod(
      Opcodes.ACC_PRIVATE,
      s"node${subplanToIdx.size + 1}",
      Seq(classOf[Array[Node]].asType, classOf[JobContext].asType)) { implicit mb =>

        val thisVar :: nodesVar :: jobContextVar :: _ = mb.argVars

        nodesVar.push().astore(ldc(subplanToIdx.size + 1), {
          val commitType = DirectOutputCommitCompiler.compile(directOutputs.map(_._2))
          val commit = pushNew(commitType)
          commit.dup().invokeInit(
            buildSet { builder =>
              directOutputs.toSeq.map(_._1).map(subplanToIdx).sorted.foreach { i =>
                builder += nodesVar.push().aload(ldc(i))
              }
            },
            jobContextVar.push())
          commit
        })
        `return`()
      }
  }

  private def defNode(
    methodDef: MethodDef)(
      subplan: SubPlan): Unit = {
    val compiler = NodeCompiler.get(subplan)(context.nodeCompilerContext)
    val nodeType = compiler.compile(subplan)(context.nodeCompilerContext)

    methodDef.newMethod(
      Opcodes.ACC_PRIVATE,
      s"node${subplanToIdx(subplan)}",
      Seq(
        classOf[Array[Node]].asType,
        classOf[mutable.Map[BroadcastId, Broadcast[_]]].asType,
        classOf[JobContext].asType)) { implicit mb =>

        val thisVar :: nodesVar :: allBroadcastsVar :: jobContextVar :: _ = mb.argVars

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
                builder += (
                  context.broadcastIds.getField(marker),
                  newBroadcast(
                    marker,
                    subplan,
                    broadcastInfo)(
                      () => buildSeq { builder =>
                        prevSubPlanOutputs.foreach { subPlanOutput =>
                          builder += tuple2(
                            nodesVar.push().aload(ldc(subplanToIdx(subPlanOutput.getOwner))),
                            context.branchKeys.getField(subPlanOutput.getOperator))
                        }
                      },
                      jobContextVar.push))
              }
            }
          }.store()

        val setupVar = if (useDirectOut) {
          Some(nodesVar.push().aload(ldc(0)).store())
        } else {
          None
        }
        val instantiator = compiler.instantiator
        val nodeVar = instantiator.newInstance(
          nodeType,
          subplan,
          subplanToIdx)(
            Instantiator.Vars(jobContextVar, nodesVar, broadcastsVar, setupVar))(
              implicitly, context.instantiatorCompilerContext)
        nodesVar.push().astore(ldc(subplanToIdx(subplan)), nodeVar.push())

        for {
          subPlanOutput <- subplan.getOutputs
          outputInfo <- Option(subPlanOutput.getAttribute(classOf[SubPlanOutputInfo]))
          if outputInfo.getOutputType == SubPlanOutputInfo.OutputType.BROADCAST
          broadcastInfo <- Option(subPlanOutput.getAttribute(classOf[BroadcastInfo]))
          if subPlanOutput.getOpposites.exists(_.getOpposites.size == 1)
        } {
          val marker = subPlanOutput.getOperator
          addToMap(
            allBroadcastsVar.push(),
            context.broadcastIds.getField(subPlanOutput.getOperator),
            newBroadcast(
              marker,
              subplan,
              broadcastInfo)(
                () => buildSeq { builder =>
                  builder += tuple2(
                    nodeVar.push().asType(classOf[Source].asType),
                    context.branchKeys.getField(marker))
                },
                jobContextVar.push))
        }
        `return`()
      }
  }

  private def newBroadcast(
    marker: MarkerOperator,
    subplan: SubPlan,
    broadcastInfo: BroadcastInfo)(
      nodes: () => Stack,
      sc: () => Stack)(
        implicit mb: MethodBuilder): Stack = {
    val dataModelRef = marker.getInput.dataModelRef
    val group = broadcastInfo.getFormatInfo
    val broadcast = pushNew(classOf[MapBroadcastOnce].asType)
    val label = Seq(
      Option(subplan.getAttribute(classOf[SubPlanInfo]))
        .flatMap(info => Option(info.getLabel)),
      Option(subplan.getAttribute(classOf[NameInfo]))
        .map(_.getName))
      .flatten match {
      case Seq() => "N/A"
      case s => s.mkString(":")
    }
    broadcast.dup().invokeInit(
      nodes(),
      option(
        sortOrdering(
          dataModelRef.groupingTypes(group.getGrouping),
          dataModelRef.orderingTypes(group.getOrdering))),
      groupingOrdering(dataModelRef.groupingTypes(group.getGrouping)),
      partitioner(ldc(1)),
      ldc(label),
      sc())
    broadcast
  }
}
