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
package graph
package branching

import scala.collection.JavaConversions._

import org.apache.spark.{ Partitioner, SparkContext }
import org.objectweb.asm.Opcodes
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.lang.compiler.planning.SubPlan
import com.asakusafw.spark.compiler.planning.SubPlanOutputInfo
import com.asakusafw.spark.compiler.util.NumPartitions._
import com.asakusafw.spark.compiler.util.SparkIdioms._
import com.asakusafw.spark.runtime.rdd.BranchKey
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._
import com.asakusafw.spark.tools.asm4s._

trait PartitionersField extends ClassBuilder {

  implicit def context: PartitionersField.Context

  def subplanOutputs: Seq[SubPlan.Output]

  override def defFields(fieldDef: FieldDef): Unit = {
    super.defFields(fieldDef)

    fieldDef.newField(
      Opcodes.ACC_PRIVATE | Opcodes.ACC_TRANSIENT,
      "partitioners",
      classOf[Map[_, _]].asType,
      new TypeSignatureBuilder()
        .newClassType(classOf[Map[_, _]].asType) {
          _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[BranchKey].asType)
            .newTypeArgument(SignatureVisitor.INSTANCEOF) {
              _.newClassType(classOf[Option[_]].asType) {
                _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[Partitioner].asType)
              }
            }
        }
        .build())
  }

  override def defMethods(methodDef: MethodDef): Unit = {
    super.defMethods(methodDef)

    methodDef.newMethod("partitioners", classOf[Map[_, _]].asType, Seq.empty,
      new MethodSignatureBuilder()
        .newReturnType {
          _.newClassType(classOf[Map[_, _]].asType) {
            _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[BranchKey].asType)
              .newTypeArgument(SignatureVisitor.INSTANCEOF) {
                _.newClassType(classOf[Option[_]].asType) {
                  _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[Partitioner].asType)
                }
              }
          }
        }
        build ()) { implicit mb =>
        val thisVar :: _ = mb.argVars
        thisVar.push().getField("partitioners", classOf[Map[_, _]].asType).unlessNotNull {
          thisVar.push().putField("partitioners", initPartitioners())
        }
        `return`(thisVar.push().getField("partitioners", classOf[Map[_, _]].asType))
      }
  }

  def getPartitionersField()(implicit mb: MethodBuilder): Stack = {
    val thisVar :: _ = mb.argVars
    thisVar.push().invokeV("partitioners", classOf[Map[_, _]].asType)
  }

  private def initPartitioners()(implicit mb: MethodBuilder): Stack = {
    val thisVar :: _ = mb.argVars

    buildMap { builder =>
      val np = numPartitions(thisVar.push().invokeV("sc", classOf[SparkContext].asType)) _
      for {
        output <- subplanOutputs.sortBy(_.getOperator.getSerialNumber)
        outputInfo <- Option(output.getAttribute(classOf[SubPlanOutputInfo]))
        if outputInfo.getOutputType == SubPlanOutputInfo.OutputType.DONT_CARE ||
          outputInfo.getOutputType == SubPlanOutputInfo.OutputType.AGGREGATED ||
          outputInfo.getOutputType == SubPlanOutputInfo.OutputType.PARTITIONED ||
          outputInfo.getOutputType == SubPlanOutputInfo.OutputType.BROADCAST
      } {
        builder += (
          context.branchKeys.getField(output.getOperator),
          outputInfo.getOutputType match {
            case SubPlanOutputInfo.OutputType.DONT_CARE =>
              pushObject(None)
            case SubPlanOutputInfo.OutputType.AGGREGATED |
              SubPlanOutputInfo.OutputType.PARTITIONED if outputInfo.getPartitionInfo.getGrouping.nonEmpty => // scalastyle:ignore
              option(partitioner(np(output)))
            case _ =>
              option(partitioner(ldc(1)))
          })
      }
    }
  }
}

object PartitionersField {

  trait Context {

    def branchKeys: BranchKeys
  }
}
