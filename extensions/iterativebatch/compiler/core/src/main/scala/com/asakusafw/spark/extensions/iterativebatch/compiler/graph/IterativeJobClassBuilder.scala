/*
 * Copyright 2011-2021 Asakusa Framework Team.
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
import scala.concurrent.{ ExecutionContext, Future }
import scala.runtime.BoxedUnit
import org.objectweb.asm.{ Opcodes, Type }
import org.objectweb.asm.signature.SignatureVisitor
import com.asakusafw.lang.compiler.extension.directio.DirectFileIoModels
import com.asakusafw.lang.compiler.model.graph.{ ExternalOutput, MarkerOperator }
import com.asakusafw.lang.compiler.planning.{ Plan, Planning, SubPlan }
import com.asakusafw.spark.compiler.graph.Instantiator
import com.asakusafw.spark.compiler.`package`._
import com.asakusafw.spark.compiler.planning._
import com.asakusafw.spark.compiler.util.SparkIdioms._
import com.asakusafw.spark.runtime.{ JobContext, RoundContext }
import com.asakusafw.spark.runtime.graph._
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._
import com.asakusafw.spark.tools.asm4s._
import com.asakusafw.utils.graph.Graphs
import com.asakusafw.spark.extensions.iterativebatch.compiler.spi.RoundAwareNodeCompiler
import com.asakusafw.spark.extensions.iterativebatch.runtime.graph._

class IterativeJobClassBuilder(
  plan: Plan)(
    implicit context: IterativeJobCompiler.Context)
  extends ClassBuilder(
    Type.getType(s"L${GeneratedClassPackageInternalName}/${context.flowId}/graph/IterativeJob;"),
    classOf[IterativeJob].asType) {

  private val directOutputs = collectDirectOutputs(plan.getElements.toSet[SubPlan])

  private def useDirectOut: Boolean = directOutputs.nonEmpty

  private val subplans = Graphs.sortPostOrder(Planning.toDependencyGraph(plan)).toSeq.zipWithIndex
  private val subplanToIdx = subplans.toMap

  override def defFields(fieldDef: FieldDef): Unit = {
    fieldDef.newField(
      Opcodes.ACC_PRIVATE | Opcodes.ACC_FINAL,
      "jobContext",
      classOf[JobContext].asType)
    fieldDef.newField(
      Opcodes.ACC_PRIVATE | Opcodes.ACC_FINAL,
      "nodes",
      classOf[Seq[Node]].asType,
      new TypeSignatureBuilder()
        .newClassType(classOf[Seq[_]].asType) {
          _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[Node].asType)
        })

    if (useDirectOut) {
      fieldDef.newField(
        Opcodes.ACC_PRIVATE | Opcodes.ACC_FINAL,
        "commit",
        classOf[DirectOutputCommitForIterative].asType)
    }
  }

  override def defConstructors(ctorDef: ConstructorDef): Unit = {
    ctorDef.newInit(Seq(
      classOf[JobContext].asType)) { implicit mb =>

      val thisVar :: jobContextVar :: _ = mb.argVars

      thisVar.push().invokeInit(
        superType,
        jobContextVar.push())

      thisVar.push().putField("jobContext", jobContextVar.push())

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

      if (useDirectOut) {
        val setupVar = thisVar.push()
          .invokeV("setup", classOf[DirectOutputSetupForIterative].asType)
          .store()

        val preparesVar = buildSet { builder =>
          (0 until directOutputs.size).foreach { i =>
            builder += thisVar.push()
              .invokeV(
                s"prepare${i}",
                classOf[DirectOutputPrepareForIterative[_]].asType,
                setupVar.push())
          }
        }.store()

        val commitVar = thisVar.push()
          .invokeV("commit", classOf[DirectOutputCommitForIterative].asType, preparesVar.push())
          .store()

        thisVar.push().putField(
          "commit",
          commitVar.push().asType(classOf[DirectOutputCommitForIterative].asType))
      }
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

    methodDef.newMethod("doCommit", classOf[Future[Unit]].asType,
      Seq(
        classOf[RoundContext].asType,
        classOf[Seq[RoundContext]].asType,
        classOf[ExecutionContext].asType),
      new MethodSignatureBuilder()
        .newParameterType(classOf[RoundContext].asType)
        .newParameterType {
          _.newClassType(classOf[Seq[_]].asType) {
            _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[RoundContext].asType)
          }
        }
        .newParameterType(classOf[ExecutionContext].asType)
        .newReturnType {
          _.newClassType(classOf[Future[_]].asType) {
            _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[BoxedUnit].asType)
          }
        }) { implicit mb =>

        val thisVar :: originVar :: rcsVar :: ecVar :: _ = mb.argVars

        if (useDirectOut) {
          `return`(
            thisVar.push().getField("commit", classOf[DirectOutputCommitForIterative].asType)
              .invokeV(
                "perform",
                classOf[Future[Unit]].asType,
                originVar.push(), rcsVar.push(), ecVar.push()))
        } else {
          `return`(
            pushObject(Future)
              .invokeV("successful", classOf[Future[_]].asType,
                getStatic(classOf[BoxedUnit].asType, "UNIT", classOf[BoxedUnit].asType)
                  .asType(classOf[AnyRef].asType)))
        }
      }

    subplans.foreach {
      case (subplan, i) =>
        methodDef.newMethod(
          Opcodes.ACC_PRIVATE,
          s"node${i}",
          Seq(classOf[Array[Node]].asType, classOf[mutable.Map[BroadcastId, Broadcast[_]]].asType))(
            defNodeMethod(subplan, i)(_))
    }

    if (useDirectOut) {
      methodDef.newMethod(
        Opcodes.ACC_PRIVATE,
        "setup",
        classOf[DirectOutputSetupForIterative].asType,
        Seq.empty) { implicit mb =>

          val thisVar :: _ = mb.argVars

          val t = DirectOutputSetupForIterativeCompiler.compile(directOutputs.map(_._2))
          val setup = pushNew(t)
          setup.dup().invokeInit(thisVar.push().getField("jobContext", classOf[JobContext].asType))
          `return`(setup)
        }

      directOutputs.toSeq.map(_._1).sortBy(subplanToIdx).zipWithIndex.foreach {
        case (subplan, i) =>
          methodDef.newMethod(
            Opcodes.ACC_PRIVATE,
            s"prepare${i}",
            classOf[DirectOutputPrepareForIterative[_]].asType,
            Seq(classOf[DirectOutputSetupForIterative].asType)) { implicit mb =>

              val thisVar :: setupVar :: _ = mb.argVars

              val t = DirectOutputPrepareForIterativeCompiler
                .compile(subplan)(context.nodeCompilerContext)
              val prepare = pushNew(t)
              prepare.dup().invokeInit(
                setupVar.push().asType(classOf[IterativeAction[_]].asType),
                applySeq(
                  thisVar.push().getField("nodes", classOf[Seq[_]].asType),
                  ldc(subplanToIdx(subplan)))
                  .cast(classOf[DirectOutputPrepareEachForIterative[_]].asType),
                thisVar.push().getField("jobContext", classOf[JobContext].asType))
              `return`(prepare)
            }
      }

      methodDef.newMethod(
        Opcodes.ACC_PRIVATE,
        "commit",
        classOf[DirectOutputCommitForIterative].asType,
        Seq(classOf[Set[DirectOutputPrepareForIterative[_]]].asType)) { implicit mb =>

          val thisVar :: preparesVar :: _ = mb.argVars

          val t = DirectOutputCommitForIterativeCompiler.compile(directOutputs.map(_._2))
          val commit = pushNew(t)
          commit.dup().invokeInit(
            preparesVar.push(),
            thisVar.push().getField("jobContext", classOf[JobContext].asType))
          `return`(commit)
        }
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

  private def defNodeMethod(
    subplan: SubPlan, i: Int)(
      implicit mb: MethodBuilder): Unit = {

    val thisVar :: nodesVar :: allBroadcastsVar :: _ = mb.argVars

    val jobContextVar = thisVar.push().getField("jobContext", classOf[JobContext].asType).store()

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
                subplan,
                broadcastInfo,
                iterativeInfo)(
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

    val compiler = RoundAwareNodeCompiler.get(subplan)(context.nodeCompilerContext)
    val nodeType = compiler.compile(subplan)(context.nodeCompilerContext)

    val instantiator = compiler.instantiator
    val nodeVar = instantiator.newInstance(
      nodeType,
      subplan,
      subplanToIdx)(
        Instantiator.Vars(jobContextVar, nodesVar, broadcastsVar))(
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
          subplan,
          broadcastInfo,
          iterativeInfo)(
            () => buildSeq { builder =>
              builder += tuple2(
                nodeVar.push().asType(classOf[Source].asType),
                context.branchKeys.getField(marker))
            },
            jobContextVar.push))
    }
    `return`()
  }

  private def newBroadcast(
    marker: MarkerOperator,
    subplan: SubPlan,
    broadcastInfo: BroadcastInfo,
    iterativeInfo: IterativeInfo)(
      nodes: () => Stack,
      jobContext: () => Stack)(
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
    val label = Seq(
      Option(subplan.getAttribute(classOf[SubPlanInfo]))
        .flatMap(info => Option(info.getLabel)),
      Option(subplan.getAttribute(classOf[NameInfo]))
        .map(_.getName))
      .flatten match {
      case Seq() => "N/A"
      case s: Seq[String] => s.mkString(":")
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
    arguments += ldc(label)
    if (iterativeInfo.getRecomputeKind == IterativeInfo.RecomputeKind.PARAMETER) {
      arguments +=
        buildSet { builder =>
          iterativeInfo.getParameters.foreach { parameter =>
            builder += ldc(parameter)
          }
        }
    }
    arguments += jobContext()
    broadcast.invokeInit(arguments.result: _*)
    broadcast
  }
}
