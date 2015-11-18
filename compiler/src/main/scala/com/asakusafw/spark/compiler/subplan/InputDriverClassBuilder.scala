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
package subplan

import java.util.concurrent.atomic.{ AtomicInteger, AtomicLong }

import scala.concurrent.Future

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.objectweb.asm.Type
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.lang.compiler.model.graph.ExternalInput
import com.asakusafw.lang.compiler.planning.SubPlan
import com.asakusafw.spark.compiler.operator.FragmentGraphBuilder
import com.asakusafw.spark.compiler.subplan.InputDriverClassBuilder._
import com.asakusafw.spark.compiler.spi.{ OperatorCompiler, SubPlanCompiler }
import com.asakusafw.spark.runtime.driver.{ BroadcastId, InputDriver }
import com.asakusafw.spark.runtime.fragment.{ Fragment, OutputFragment }
import com.asakusafw.spark.runtime.rdd.BranchKey
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm4s._

class InputDriverClassBuilder(
  val operator: ExternalInput,
  val inputFormatType: Type,
  val keyType: Type,
  val valueType: Type,
  val paths: Option[Seq[String]],
  val extraConfigurations: Option[Map[String, String]])(
    val label: String,
    val subplanOutputs: Seq[SubPlan.Output])(
      implicit val context: SubPlanCompiler.Context)
  extends ClassBuilder(
    Type.getType(
      s"L${GeneratedClassPackageInternalName}/${context.flowId}/driver/InputDriver$$${nextId};"),
    new ClassSignatureBuilder()
      .newSuperclass {
        _.newClassType(classOf[InputDriver[_, _, _]].asType) {
          _.newTypeArgument(SignatureVisitor.INSTANCEOF, inputFormatType)
            .newTypeArgument(SignatureVisitor.INSTANCEOF, keyType)
            .newTypeArgument(SignatureVisitor.INSTANCEOF, valueType)
        }
      }
      .build(),
    classOf[InputDriver[_, _, _]].asType)
  with Branching
  with LabelField {

  override def defConstructors(ctorDef: ConstructorDef): Unit = {
    ctorDef.newInit(Seq(
      classOf[SparkContext].asType,
      classOf[Broadcast[Configuration]].asType,
      classOf[Map[BroadcastId, Future[Broadcast[_]]]].asType),
      new MethodSignatureBuilder()
        .newParameterType(classOf[SparkContext].asType)
        .newParameterType {
          _.newClassType(classOf[Broadcast[_]].asType) {
            _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[Configuration].asType)
          }
        }
        .newParameterType {
          _.newClassType(classOf[Map[_, _]].asType) {
            _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[BroadcastId].asType)
              .newTypeArgument(SignatureVisitor.INSTANCEOF) {
                _.newClassType(classOf[Future[_]].asType) {
                  _.newTypeArgument(SignatureVisitor.INSTANCEOF) {
                    _.newClassType(classOf[Broadcast[_]].asType) {
                      _.newTypeArgument()
                    }
                  }
                }
              }
          }
        }
        .newVoidReturnType()
        .build()) { implicit mb =>
        import mb._ // scalastyle:ignore
        val scVar =
          `var`(classOf[SparkContext].asType, thisVar.nextLocal)
        val hadoopConfVar =
          `var`(classOf[Broadcast[Configuration]].asType, scVar.nextLocal)
        val broadcastsVar =
          `var`(classOf[Map[BroadcastId, Future[Broadcast[_]]]].asType, hadoopConfVar.nextLocal)

        thisVar.push().invokeInit(
          superType,
          scVar.push(),
          hadoopConfVar.push(),
          broadcastsVar.push(),
          classTag(inputFormatType),
          classTag(keyType),
          classTag(valueType))
      }
  }

  override def defMethods(methodDef: MethodDef): Unit = {
    super.defMethods(methodDef)

    methodDef.newMethod("paths", classOf[Option[Set[String]]].asType, Seq.empty,
      new MethodSignatureBuilder()
        .newReturnType {
          _.newClassType(classOf[Option[_]].asType) {
            _.newTypeArgument(SignatureVisitor.INSTANCEOF) {
              _.newClassType(classOf[Set[_]].asType) {
                _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[String].asType)
              }
            }
          }
        }
        .build()) { implicit mb =>
        import mb._ // scalastyle:ignore
        `return`(
          paths match {
            case Some(paths) =>
              option(
                buildSet { builder =>
                  for {
                    path <- paths
                  } {
                    builder += ldc(path)
                  }
                })
            case None =>
              pushObject(None)
          })
      }

    methodDef.newMethod("extraConfigurations", classOf[Map[String, String]].asType, Seq.empty,
      new MethodSignatureBuilder()
        .newReturnType {
          _.newClassType(classOf[Map[_, _]].asType) {
            _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[String].asType)
              .newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[String].asType)
          }
        }
        .build()) { implicit mb =>
        import mb._ // scalastyle:ignore
        `return`(
          buildMap { builder =>
            extraConfigurations.foreach {
              for {
                (k, v) <- _
              } {
                builder += (ldc(k), ldc(v))
              }
            }
          })
      }

    methodDef.newMethod(
      "fragments",
      classOf[(_, _)].asType,
      Seq(classOf[Map[BroadcastId, Broadcast[_]]].asType, Type.INT_TYPE),
      new MethodSignatureBuilder()
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
        .newParameterType(Type.INT_TYPE)
        .newReturnType {
          _.newClassType(classOf[(_, _)].asType) {
            _.newTypeArgument(SignatureVisitor.INSTANCEOF) {
              _.newClassType(classOf[Fragment[_]].asType) {
                _.newTypeArgument(SignatureVisitor.INSTANCEOF, valueType)
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
        import mb._ // scalastyle:ignore
        val broadcastsVar =
          `var`(classOf[Map[BroadcastId, Broadcast[_]]].asType, thisVar.nextLocal)
        val fragmentBufferSizeVar = `var`(Type.INT_TYPE, broadcastsVar.nextLocal)
        val nextLocal = new AtomicInteger(fragmentBufferSizeVar.nextLocal)

        val fragmentBuilder =
          new FragmentGraphBuilder(
            broadcastsVar, fragmentBufferSizeVar, nextLocal)(
            implicitly, context.operatorCompilerContext)
        val fragmentVar = fragmentBuilder.build(operator.getOperatorPort)
        val outputsVar = fragmentBuilder.buildOutputsVar(subplanOutputs)

        `return`(tuple2(fragmentVar.push(), outputsVar.push()))
      }
  }
}

object InputDriverClassBuilder {

  private[this] val curId: AtomicLong = new AtomicLong(0L)

  def nextId: Long = curId.getAndIncrement
}
