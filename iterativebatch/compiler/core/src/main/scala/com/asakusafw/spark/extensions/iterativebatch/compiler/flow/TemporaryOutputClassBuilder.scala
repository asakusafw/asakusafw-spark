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

import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.SparkContext
import org.objectweb.asm.Type
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.lang.compiler.model.graph.ExternalOutput
import com.asakusafw.lang.compiler.planning.SubPlan
import com.asakusafw.spark.compiler._
import com.asakusafw.spark.compiler.subplan.LabelField
import com.asakusafw.spark.runtime.driver.BroadcastId
import com.asakusafw.spark.runtime.rdd.BranchKey
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._

import com.asakusafw.spark.extensions.iterativebatch.compiler.flow.TemporaryOutputClassBuilder._
import com.asakusafw.spark.extensions.iterativebatch.compiler.spi.NodeCompiler
import com.asakusafw.spark.extensions.iterativebatch.runtime.flow.{
  Broadcast,
  Source,
  Target,
  TemporaryOutput
}

class TemporaryOutputClassBuilder(
  operator: ExternalOutput)(
    val label: String)(
      implicit val context: NodeCompiler.Context)
  extends ClassBuilder(
    Type.getType(
      s"L${GeneratedClassPackageInternalName}/${context.flowId}/flow/TemporaryOutput$$${nextId};"),
    new ClassSignatureBuilder()
      .newSuperclass {
        _.newClassType(classOf[TemporaryOutput[_]].asType) {
          _.newTypeArgument(SignatureVisitor.INSTANCEOF, operator.getDataType.asType)
        }
      }
      .build(),
    classOf[TemporaryOutput[_]].asType)
  with LabelField
  with ScalaIdioms {

  override def defConstructors(ctorDef: ConstructorDef): Unit = {
    ctorDef.newInit(Seq(
      classOf[Seq[Target]].asType,
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
        .newParameterType(classOf[SparkContext].asType)
        .newVoidReturnType()
        .build()) { mb =>
        import mb._ // scalastyle:ignore
        val prevsVar = `var`(classOf[Seq[Target]].asType, thisVar.nextLocal)
        val scVar = `var`(classOf[SparkContext].asType, prevsVar.nextLocal)

        thisVar.push().invokeInit(
          superType,
          prevsVar.push(),
          classTag(mb, operator.getDataType.asType),
          scVar.push())
      }
  }

  override def defMethods(methodDef: MethodDef): Unit = {
    super.defMethods(methodDef)

    methodDef.newMethod("path", classOf[String].asType, Seq.empty) { mb =>
      import mb._ // scalastyle:ignore
      `return`(ldc(context.options.getRuntimeWorkingPath(operator.getName)))
    }
  }
}

object TemporaryOutputClassBuilder {

  private[this] val curId: AtomicLong = new AtomicLong(0L)

  def nextId: Long = curId.getAndIncrement
}
