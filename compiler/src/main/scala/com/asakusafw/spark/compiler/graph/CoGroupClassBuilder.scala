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
package com.asakusafw.spark.compiler
package graph

import java.util.concurrent.atomic.AtomicLong

import scala.collection.JavaConversions._
import scala.collection.mutable

import org.apache.spark.Partitioner
import org.apache.spark.broadcast.{ Broadcast => Broadcasted }

import org.objectweb.asm.Type
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.lang.compiler.model.graph.UserOperator
import com.asakusafw.lang.compiler.planning.SubPlan
import com.asakusafw.spark.compiler.graph.branching.Branching
import com.asakusafw.spark.compiler.graph.CoGroupClassBuilder._
import com.asakusafw.spark.compiler.operator.FragmentGraphBuilder
import com.asakusafw.spark.compiler.spi.{ NodeCompiler, OperatorCompiler, OperatorType }
import com.asakusafw.spark.runtime.JobContext
import com.asakusafw.spark.runtime.fragment.{ Fragment, OutputFragment }
import com.asakusafw.spark.runtime.graph.{
  Broadcast,
  BroadcastId,
  CoGroup,
  GroupOrdering,
  SortOrdering,
  Source
}
import com.asakusafw.spark.runtime.rdd.{ BranchKey, ShuffleKey }
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._
import com.asakusafw.spark.tools.asm4s._

abstract class CoGroupClassBuilder(
  operator: UserOperator)(
    val label: String,
    val subplanOutputs: Seq[SubPlan.Output])(
      implicit val context: NodeCompiler.Context)
  extends ClassBuilder(
    Type.getType(
      s"L${GeneratedClassPackageInternalName}/${context.flowId}/graph/CoGroup$$${nextId};"),
    classOf[CoGroup].asType)
  with Branching
  with LabelField {
  self: CacheStrategy =>

  override def defConstructors(ctorDef: ConstructorDef): Unit = {
    ctorDef.newInit(Seq(
      classOf[Seq[(Seq[(Source, BranchKey)], Option[SortOrdering])]].asType,
      classOf[GroupOrdering].asType,
      classOf[Partitioner].asType,
      classOf[Map[BroadcastId, Broadcast[_]]].asType,
      classOf[JobContext].asType),
      new MethodSignatureBuilder()
        .newParameterType {
          _.newClassType(classOf[Seq[_]].asType) {
            _.newTypeArgument(SignatureVisitor.INSTANCEOF) {
              _.newClassType(classOf[(_, _)].asType) {
                _.newTypeArgument(SignatureVisitor.INSTANCEOF) {
                  _.newClassType(classOf[Seq[_]].asType) {
                    _.newTypeArgument(SignatureVisitor.INSTANCEOF) {
                      _.newClassType(classOf[(_, _)].asType) {
                        _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[Source].asType)
                          .newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[BranchKey].asType)
                      }
                    }
                  }
                }
                  .newTypeArgument(SignatureVisitor.INSTANCEOF) {
                    _.newClassType(classOf[Option[_]].asType) {
                      _.newTypeArgument(SignatureVisitor.INSTANCEOF) {
                        _.newClassType(classOf[Ordering[_]].asType) {
                          _.newTypeArgument(
                            SignatureVisitor.INSTANCEOF, classOf[ShuffleKey].asType)
                        }
                      }
                    }
                  }
              }
            }
          }
        }
        .newParameterType {
          _.newClassType(classOf[Ordering[_]].asType) {
            _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[ShuffleKey].asType)
          }
        }
        .newParameterType(classOf[Partitioner].asType)
        .newParameterType {
          _.newClassType(classOf[Map[_, _]].asType) {
            _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[BroadcastId].asType)
              .newTypeArgument(SignatureVisitor.INSTANCEOF) {
                _.newClassType(classOf[Broadcast[_]].asType) {
                  _.newTypeArgument()
                }
              }
          }
        }
        .newParameterType(classOf[JobContext].asType)
        .newVoidReturnType()) { implicit mb =>

        val (thisVar :: prevsVar :: groupingVar :: partVar :: broadcastsVar
          :: jobContextVar :: _) = mb.argVars

        thisVar.push().invokeInit(
          superType,
          prevsVar.push(),
          groupingVar.push(),
          partVar.push(),
          broadcastsVar.push(),
          jobContextVar.push())
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
                _.newTypeArgument(SignatureVisitor.INSTANCEOF) {
                  _.newClassType(classOf[Seq[_]].asType) {
                    _.newTypeArgument(SignatureVisitor.INSTANCEOF) {
                      _.newClassType(classOf[Iterable[_]].asType) {
                        _.newTypeArgument()
                      }
                    }
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
        }) { implicit mb =>

        val thisVar :: broadcastsVar :: fragmentBufferSizeVar :: _ = mb.argVars

        val fragmentBuilder =
          new FragmentGraphBuilder(
            broadcastsVar, fragmentBufferSizeVar)(
            implicitly, context.operatorCompilerContext)
        val fragmentVar = {
          val t =
            OperatorCompiler.compile(
              operator, OperatorType.CoGroupType)(
                context.operatorCompilerContext)
          val outputs = operator.getOutputs.map(fragmentBuilder.build)
          val fragment = pushNew(t)
          fragment.dup().invokeInit(
            broadcastsVar.push()
              +: outputs.map(_.push().asType(classOf[Fragment[_]].asType)): _*)
          fragment.store()
        }
        val outputsVar = fragmentBuilder.buildOutputsVar(subplanOutputs)

        `return`(tuple2(fragmentVar.push(), outputsVar.push()))
      }
  }
}

object CoGroupClassBuilder {

  private[this] val curIds: mutable.Map[NodeCompiler.Context, AtomicLong] =
    mutable.WeakHashMap.empty

  def nextId(implicit context: NodeCompiler.Context): Long =
    curIds.getOrElseUpdate(context, new AtomicLong(0)).getAndIncrement()
}
