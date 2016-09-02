/*
 * Copyright 2011-2016 Asakusa Framework Team.
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

import java.util.concurrent.atomic.AtomicLong

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.runtime.BoxedUnit

import org.apache.spark.Partitioner
import org.objectweb.asm.{ Opcodes, Type }
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.lang.compiler.extension.directio.{ DirectFileOutputModel, OutputPattern }
import com.asakusafw.lang.compiler.model.graph.{ ExternalOutput, Group }
import com.asakusafw.runtime.directio.DataFormat
import com.asakusafw.runtime.value.StringOption
import com.asakusafw.spark.compiler._
import com.asakusafw.spark.compiler.graph.CacheStrategy
import com.asakusafw.spark.compiler.spi.NodeCompiler
import com.asakusafw.spark.compiler.util.SparkIdioms._
import com.asakusafw.spark.runtime.JobContext
import com.asakusafw.spark.runtime.graph.SortOrdering
import com.asakusafw.spark.runtime.rdd.ShuffleKey
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._
import com.asakusafw.spark.tools.asm4s._

import com.asakusafw.spark.extensions.iterativebatch.compiler.graph.DirectOutputPrepareForIterativeClassBuilder._ // scalastyle:ignore
import com.asakusafw.spark.extensions.iterativebatch.runtime.graph.{
  DirectOutputPrepareEachForIterative,
  DirectOutputPrepareForIterative,
  IterativeAction
}

abstract class DirectOutputPrepareForIterativeClassBuilder(
  operator: ExternalOutput)(
    pattern: OutputPattern,
    model: DirectFileOutputModel)(
      val label: String)(
        implicit val context: NodeCompiler.Context)
  extends ClassBuilder(
    Type.getType(
      s"L${GeneratedClassPackageInternalName}/${context.flowId}/graph/DirectOutputPrepareForIterative$$${nextId};"), // scalastyle:ignore
    new ClassSignatureBuilder()
      .newSuperclass {
        _.newClassType(classOf[DirectOutputPrepareForIterative[_]].asType) {
          _.newTypeArgument(SignatureVisitor.INSTANCEOF, operator.getOperatorPort.dataModelType)
        }
      },
    classOf[DirectOutputPrepareForIterative[_]].asType) {
  self: CacheStrategy =>

  private val dataModelType = operator.getOperatorPort.dataModelType

  override def defConstructors(ctorDef: ConstructorDef): Unit = {
    ctorDef.newInit(Seq(
      classOf[IterativeAction[Unit]].asType,
      classOf[DirectOutputPrepareEachForIterative[_]].asType,
      classOf[JobContext].asType),
      new MethodSignatureBuilder()
        .newParameterType {
          _.newClassType(classOf[IterativeAction[_]].asType) {
            _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[BoxedUnit].asType)
          }
        }
        .newParameterType {
          _.newClassType(classOf[DirectOutputPrepareEachForIterative[_]].asType) {
            _.newTypeArgument(SignatureVisitor.INSTANCEOF, dataModelType)
          }
        }
        .newParameterType(classOf[JobContext].asType)
        .newVoidReturnType()) { implicit mb =>

        val thisVar :: setupVar :: prepareVar :: jobContextVar :: _ = mb.argVars

        thisVar.push().invokeInit(
          superType,
          setupVar.push(),
          prepareVar.push(),
          manifest(dataModelType),
          jobContextVar.push())
        initMixIns()
      }
  }

  override def defMethods(methodDef: MethodDef): Unit = {
    super.defMethods(methodDef)

    methodDef.newMethod("label", classOf[String].asType, Seq.empty) { implicit mb =>
      `return`(ldc(s"epilogue: ${label}"))
    }

    methodDef.newMethod("formatType", classOf[Class[_ <: DataFormat[_]]].asType, Seq.empty,
      new MethodSignatureBuilder()
        .newReturnType {
          _.newClassType(classOf[Class[_]].asType) {
            _.newTypeArgument(SignatureVisitor.EXTENDS) {
              _.newClassType(classOf[DataFormat[_]].asType) {
                _.newTypeArgument(SignatureVisitor.INSTANCEOF, dataModelType)
              }
            }
          }
        }) { implicit mb =>
        `return`(ldc(model.getFormatClass.asType).asType(classOf[Class[_]].asType))
      }
  }
}

object DirectOutputPrepareForIterativeClassBuilder {

  private[this] val curIds: mutable.Map[NodeCompiler.Context, AtomicLong] =
    mutable.WeakHashMap.empty

  def nextId(implicit context: NodeCompiler.Context): Long =
    curIds.getOrElseUpdate(context, new AtomicLong(0L)).getAndIncrement()
}
