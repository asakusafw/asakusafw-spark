/*
 * Copyright 2011-2019 Asakusa Framework Team.
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

import scala.collection.mutable

import org.objectweb.asm.Type
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.lang.compiler.model.graph.ExternalOutput
import com.asakusafw.spark.compiler.graph.TemporaryOutputClassBuilder._
import com.asakusafw.spark.compiler.spi.NodeCompiler
import com.asakusafw.spark.runtime.JobContext
import com.asakusafw.spark.runtime.graph.{
  Broadcast,
  Source,
  TemporaryOutput
}
import com.asakusafw.spark.runtime.rdd.BranchKey
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._
import com.asakusafw.spark.tools.asm4s._

class TemporaryOutputClassBuilder(
  operator: ExternalOutput)(
    val label: String)(
      implicit val context: NodeCompiler.Context)
  extends ClassBuilder(
    Type.getType(
      s"L${GeneratedClassPackageInternalName}/${context.flowId}/graph/TemporaryOutput$$${nextId};"),
    new ClassSignatureBuilder()
      .newSuperclass {
        _.newClassType(classOf[TemporaryOutput[_]].asType) {
          _.newTypeArgument(SignatureVisitor.INSTANCEOF, operator.getDataType.asType)
        }
      },
    classOf[TemporaryOutput[_]].asType)
  with LabelField {

  override def defConstructors(ctorDef: ConstructorDef): Unit = {
    ctorDef.newInit(Seq(
      classOf[Seq[(Source, BranchKey)]].asType,
      classOf[JobContext].asType),
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
        .newParameterType(classOf[JobContext].asType)
        .newVoidReturnType()) { implicit mb =>

        val thisVar :: prevsVar :: jobContextVar :: _ = mb.argVars

        thisVar.push().invokeInit(
          superType,
          prevsVar.push(),
          classTag(operator.getDataType.asType),
          jobContextVar.push())
      }
  }

  override def defMethods(methodDef: MethodDef): Unit = {
    super.defMethods(methodDef)

    methodDef.newMethod("name", classOf[String].asType, Seq.empty) { implicit mb =>
      `return`(ldc(operator.getName))
    }

    methodDef.newMethod("path", classOf[String].asType, Seq.empty) { implicit mb =>
      `return`(ldc(context.options.getRuntimeWorkingPath(operator.getName)))
    }
  }
}

object TemporaryOutputClassBuilder {

  private[this] val curIds: mutable.Map[NodeCompiler.Context, AtomicLong] =
    mutable.WeakHashMap.empty

  def nextId(implicit context: NodeCompiler.Context): Long =
    curIds.getOrElseUpdate(context, new AtomicLong(0L)).getAndIncrement()
}
