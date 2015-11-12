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
package flow

import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.broadcast.{ Broadcast => Broadcasted }
import org.objectweb.asm.Type
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.lang.compiler.model.graph.ExternalInput
import com.asakusafw.lang.compiler.planning.SubPlan
import com.asakusafw.spark.compiler.operator.FragmentGraphBuilder
import com.asakusafw.spark.compiler.subplan.{ Branching, LabelField }
import com.asakusafw.spark.compiler.util.ScalaIdioms._
import com.asakusafw.spark.runtime.driver.BroadcastId
import com.asakusafw.spark.runtime.fragment.{ Fragment, OutputFragment }
import com.asakusafw.spark.runtime.rdd.BranchKey
import com.asakusafw.spark.tools.asm._

import com.asakusafw.spark.extensions.iterativebatch.compiler.spi.NodeCompiler
import com.asakusafw.spark.extensions.iterativebatch.compiler.util.{ MixIn, Mixing }

abstract class NewHadoopInputClassBuilder(
  val operator: ExternalInput,
  val valueType: Type,
  computeStrategy: MixIn)(
    val label: String,
    val subplanOutputs: Seq[SubPlan.Output])(
      thisType: Type,
      signature: String,
      superType: Type)(
        implicit val context: NodeCompiler.Context)
  extends ClassBuilder(thisType, signature, superType,
    computeStrategy.traitType)
  with Branching
  with LabelField
  with Mixing {

  override val mixins: Seq[MixIn] = Seq(computeStrategy)

  override def defMethods(methodDef: MethodDef): Unit = {
    super.defMethods(methodDef)

    methodDef.newMethod(
      "fragments",
      classOf[(_, _)].asType,
      Seq(classOf[Map[BroadcastId, Broadcasted[_]]].asType, Type.INT_TYPE),
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
        .newParameterType(Type.INT_TYPE)
        .newReturnType {
          _.newClassType(classOf[(_, _)].asType) {
            _.newTypeArgument(SignatureVisitor.INSTANCEOF) {
              _.newClassType(classOf[Fragment[_]].asType) {
                _.newTypeArgument(SignatureVisitor.INSTANCEOF) {
                  _.newClassType(classOf[Seq[_]].asType) {
                    _.newTypeArgument(SignatureVisitor.INSTANCEOF, valueType)
                  }
                }
              }
            }
              .newTypeArgument(SignatureVisitor.INSTANCEOF) {
                _.newClassType(classOf[Map[_, _]].asType) {
                  _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[BranchKey].asType)
                    .newTypeArgument(SignatureVisitor.INSTANCEOF) {
                      _.newClassType(classOf[OutputFragment[_]].asType) {
                        _.newTypeArgument()
                      }
                    }
                }
              }
          }
        }
        .build()) { mb =>
        import mb._ // scalastyle:ignore
        val broadcastsVar =
          `var`(classOf[Map[BroadcastId, Broadcasted[_]]].asType, thisVar.nextLocal)
        val fragmentBufferSizeVar = `var`(Type.INT_TYPE, broadcastsVar.nextLocal)
        val nextLocal = new AtomicInteger(fragmentBufferSizeVar.nextLocal)

        val fragmentBuilder =
          new FragmentGraphBuilder(
            mb, broadcastsVar, fragmentBufferSizeVar, nextLocal)(
            context.operatorCompilerContext)
        val fragmentVar = fragmentBuilder.build(operator.getOperatorPort)
        val outputsVar = fragmentBuilder.buildOutputsVar(subplanOutputs)

        `return`(tuple2(mb)(fragmentVar.push(), outputsVar.push()))
      }
  }
}
