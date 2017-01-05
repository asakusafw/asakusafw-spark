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
package branching

import org.apache.spark.broadcast.{ Broadcast => Broadcasted }
import org.objectweb.asm.Opcodes
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.lang.compiler.model.graph._
import com.asakusafw.lang.compiler.planning.SubPlan
import com.asakusafw.spark.compiler.operator.aggregation.AggregationClassBuilder
import com.asakusafw.spark.compiler.planning.SubPlanOutputInfo
import com.asakusafw.spark.compiler.spi.AggregationCompiler
import com.asakusafw.spark.runtime.aggregation.Aggregation
import com.asakusafw.spark.runtime.graph.BroadcastId
import com.asakusafw.spark.runtime.rdd.{ BranchKey, ShuffleKey }
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._
import com.asakusafw.spark.tools.asm4s._

trait Aggregations extends ClassBuilder {

  implicit def context: Aggregations.Context

  def subplanOutputs: Seq[SubPlan.Output]

  override def defMethods(methodDef: MethodDef): Unit = {
    super.defMethods(methodDef)

    methodDef.newMethod(
      "aggregations",
      classOf[Map[_, _]].asType,
      Seq(classOf[Map[BroadcastId, Broadcasted[_]]].asType),
      new MethodSignatureBuilder()
        .newParameterType {
          _.newClassType(classOf[Map[_, _]].asType) {
            _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[BroadcastId].asType)
              .newTypeArgument(SignatureVisitor.INSTANCEOF) {
                _.newClassType(classOf[Broadcasted[_]].asType) {
                  _.newTypeArgument()
                }
              }
          }
        }
        .newReturnType {
          _.newClassType(classOf[Map[_, _]].asType) {
            _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[BranchKey].asType)
              .newTypeArgument(SignatureVisitor.INSTANCEOF) {
                _.newClassType(classOf[Aggregation[_, _, _]].asType) {
                  _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[ShuffleKey].asType)
                    .newTypeArgument()
                    .newTypeArgument()
                }
              }
          }
        }) { implicit mb =>

        val thisVar :: broadcastsVar :: _ = mb.argVars

        `return`(
          buildMap { builder =>
            for {
              output <- subplanOutputs.sortBy(_.getOperator.getSerialNumber)
              outputInfo <- Option(output.getAttribute(classOf[SubPlanOutputInfo]))
              if outputInfo.getOutputType == SubPlanOutputInfo.OutputType.AGGREGATED
              operator = outputInfo.getAggregationInfo.asInstanceOf[UserOperator]
              if AggregationCompiler.support(operator)(context.aggregationCompilerContext)
            } {
              val aggregationType =
                AggregationClassBuilder.getOrCompile(operator)(context.aggregationCompilerContext)

              builder += (
                context.branchKeys.getField(output.getOperator), {
                  val aggregation = pushNew(aggregationType)
                  aggregation.dup().invokeInit(broadcastsVar.push())
                  aggregation
                })
            }
          })
      }
  }
}

object Aggregations {

  trait Context
    extends ClassLoaderProvider {

    def branchKeys: BranchKeys

    def aggregationCompilerContext: AggregationCompiler.Context
  }
}
