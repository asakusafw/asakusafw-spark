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

import org.objectweb.asm.Opcodes
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.lang.compiler.planning.SubPlan
import com.asakusafw.spark.compiler.planning.{ BroadcastInfo, SubPlanOutputInfo }
import com.asakusafw.spark.compiler.util.SparkIdioms._
import com.asakusafw.spark.runtime.rdd.{ BranchKey, ShuffleKey }
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._
import com.asakusafw.spark.tools.asm4s._

trait OrderingsField extends ClassBuilder {

  implicit def context: OrderingsField.Context

  def subplanOutputs: Seq[SubPlan.Output]

  override def defFields(fieldDef: FieldDef): Unit = {
    super.defFields(fieldDef)

    fieldDef.newField(
      Opcodes.ACC_PRIVATE | Opcodes.ACC_TRANSIENT,
      "orderings",
      classOf[Map[_, _]].asType,
      new TypeSignatureBuilder()
        .newClassType(classOf[Map[_, _]].asType) {
          _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[BranchKey].asType)
            .newTypeArgument(SignatureVisitor.INSTANCEOF) {
              _.newClassType(classOf[Ordering[_]].asType) {
                _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[ShuffleKey].asType)
              }
            }
        })
  }

  override def defMethods(methodDef: MethodDef): Unit = {
    super.defMethods(methodDef)

    methodDef.newMethod("orderings", classOf[Map[_, _]].asType, Seq.empty,
      new MethodSignatureBuilder()
        .newReturnType {
          _.newClassType(classOf[Map[_, _]].asType) {
            _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[BranchKey].asType)
              .newTypeArgument(SignatureVisitor.INSTANCEOF) {
                _.newClassType(classOf[Ordering[_]].asType) {
                  _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[ShuffleKey].asType)
                }
              }
          }
        }) { implicit mb =>
        val thisVar :: _ = mb.argVars
        thisVar.push().getField("orderings", classOf[Map[_, _]].asType).unlessNotNull {
          thisVar.push().putField("orderings", initOrderings())
        }
        `return`(thisVar.push().getField("orderings", classOf[Map[_, _]].asType))
      }
  }

  def getOrderingsField()(implicit mb: MethodBuilder): Stack = {
    val thisVar :: _ = mb.argVars
    thisVar.push().invokeV("orderings", classOf[Map[_, _]].asType)
  }

  private def initOrderings()(implicit mb: MethodBuilder): Stack = {
    buildMap { builder =>
      for {
        output <- subplanOutputs.sortBy(_.getOperator.getSerialNumber)
        outputInfo <- Option(output.getAttribute(classOf[SubPlanOutputInfo]))
        partitionInfo <- outputInfo.getOutputType match {
          case SubPlanOutputInfo.OutputType.AGGREGATED | SubPlanOutputInfo.OutputType.PARTITIONED =>
            Option(outputInfo.getPartitionInfo)
          case SubPlanOutputInfo.OutputType.BROADCAST =>
            Option(output.getAttribute(classOf[BroadcastInfo])).map(_.getFormatInfo)
          case _ => None
        }
      } {
        val dataModelRef = output.getOperator.getInput.dataModelRef
        builder += (
          context.branchKeys.getField(output.getOperator),
          sortOrdering(
            dataModelRef.groupingTypes(partitionInfo.getGrouping),
            dataModelRef.orderingTypes(partitionInfo.getOrdering)))
      }
    }
  }
}

object OrderingsField {

  trait Context
    extends CompilerContext
    with DataModelLoaderProvider {

    def branchKeys: BranchKeys
  }
}
