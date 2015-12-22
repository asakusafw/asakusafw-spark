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

import java.util.concurrent.atomic.AtomicLong

import scala.collection.JavaConversions._
import scala.concurrent.Future

import org.apache.spark.{ Partitioner, SparkContext }
import org.apache.spark.broadcast.{ Broadcast => Broadcasted }
import org.objectweb.asm.Type
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.lang.compiler.model.graph.UserOperator
import com.asakusafw.lang.compiler.planning.SubPlan
import com.asakusafw.spark.compiler.graph.AggregateClassBuilder._
import com.asakusafw.spark.compiler.graph.branching.Branching
import com.asakusafw.spark.compiler.operator.FragmentGraphBuilder
import com.asakusafw.spark.compiler.operator.aggregation.AggregationClassBuilder
import com.asakusafw.spark.compiler.spi.NodeCompiler
import com.asakusafw.spark.runtime.aggregation.Aggregation
import com.asakusafw.spark.runtime.fragment.{ Fragment, OutputFragment }
import com.asakusafw.spark.runtime.graph.{
  Aggregate,
  Broadcast,
  BroadcastId,
  SortOrdering,
  Source
}
import com.asakusafw.spark.runtime.rdd.{ BranchKey, ShuffleKey }
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._
import com.asakusafw.spark.tools.asm4s._

abstract class AggregateClassBuilder(
  val valueType: Type,
  val combinerType: Type,
  val operator: UserOperator)(
    val label: String,
    val subplanOutputs: Seq[SubPlan.Output])(
      implicit val context: NodeCompiler.Context)
  extends ClassBuilder(
    Type.getType(
      s"L${GeneratedClassPackageInternalName}/${context.flowId}/graph/Aggregate$$${nextId};"), // scalastyle:ignore
    new ClassSignatureBuilder()
      .newSuperclass {
        _.newClassType(classOf[Aggregate[_, _]].asType) {
          _
            .newTypeArgument(SignatureVisitor.INSTANCEOF, valueType)
            .newTypeArgument(SignatureVisitor.INSTANCEOF, combinerType)
        }
      },
    classOf[Aggregate[_, _]].asType)
  with Branching
  with LabelField {
  self: ComputationStrategy =>

  override def defConstructors(ctorDef: ConstructorDef): Unit = {
    ctorDef.newInit(Seq(
      classOf[Seq[(Source, BranchKey)]].asType,
      classOf[Option[SortOrdering]].asType,
      classOf[Partitioner].asType,
      classOf[Map[BroadcastId, Future[Broadcast]]].asType,
      classOf[SparkContext].asType),
      new MethodSignatureBuilder()
        .newParameterType {
          _.newClassType(classOf[Seq[_]].asType) {
            _.newTypeArgument(SignatureVisitor.INSTANCEOF) {
              _.newClassType(classOf[(_, _)].asType) {
                _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[Source].asType)
                  .newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[BranchKey].asType)
              }
            }
          }
        }
        .newParameterType {
          _.newClassType(classOf[Option[_]].asType) {
            _.newTypeArgument(SignatureVisitor.INSTANCEOF) {
              _.newClassType(classOf[Ordering[_]].asType) {
                _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[ShuffleKey].asType)
              }
            }
          }
        }
        .newParameterType(classOf[Partitioner].asType)
        .newParameterType {
          _.newClassType(classOf[Map[_, _]].asType) {
            _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[BroadcastId].asType)
              .newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[Broadcast].asType)
          }
        }
        .newParameterType(classOf[SparkContext].asType)
        .newVoidReturnType()
        .build()) { implicit mb =>

        val thisVar :: prevsVar :: sortVar :: partVar :: broadcastsVar :: scVar :: _ = mb.argVars

        thisVar.push().invokeInit(
          superType,
          prevsVar.push(),
          sortVar.push(),
          partVar.push(),
          broadcastsVar.push(),
          classTag(valueType),
          classTag(combinerType),
          scVar.push())
        initMixIns()
      }
  }

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
                _.newTypeArgument(SignatureVisitor.INSTANCEOF, combinerType)
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
        .build()) { implicit mb =>

        val thisVar :: broadcastsVar :: fragmentBufferSizeVar :: _ = mb.argVars

        val fragmentBuilder =
          new FragmentGraphBuilder(
            broadcastsVar, fragmentBufferSizeVar)(
            implicitly, context.operatorCompilerContext)
        val fragmentVar = fragmentBuilder.build(operator.getOutputs.head)
        val outputsVar = fragmentBuilder.buildOutputsVar(subplanOutputs)

        `return`(tuple2(fragmentVar.push(), outputsVar.push()))
      }

    methodDef.newMethod("aggregation", classOf[Aggregation[_, _, _]].asType, Seq.empty,
      new MethodSignatureBuilder()
        .newReturnType {
          _.newClassType(classOf[Aggregation[_, _, _]].asType) {
            _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[ShuffleKey].asType)
              .newTypeArgument(SignatureVisitor.INSTANCEOF, valueType)
              .newTypeArgument(SignatureVisitor.INSTANCEOF, combinerType)
          }
        }
        .build()) { implicit mb =>

        val aggregationType =
          AggregationClassBuilder.getOrCompile(operator)(context.aggregationCompilerContext)
        `return`(pushNew0(aggregationType))
      }
  }
}

object AggregateClassBuilder {

  private[this] val curId: AtomicLong = new AtomicLong(0L)

  def nextId: Long = curId.getAndIncrement
}
