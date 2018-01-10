/*
 * Copyright 2011-2018 Asakusa Framework Team.
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

import scala.runtime.BoxedUnit

import org.objectweb.asm.Type
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.spark.compiler._
import com.asakusafw.spark.compiler.graph.CacheStrategy
import com.asakusafw.spark.runtime.JobContext
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._
import com.asakusafw.spark.tools.asm4s._

import com.asakusafw.spark.extensions.iterativebatch.runtime.graph.{
  IterativeAction,
  DirectOutputCommitForIterative
}

class DirectOutputCommitForIterativeClassBuilder(
  basePaths: Set[String])(
    implicit val context: CompilerContext)
  extends ClassBuilder(
    Type.getType(
      s"L${GeneratedClassPackageInternalName}/${context.flowId}/graph/DirectOutputCommitForIterative;"), // scalastyle:ignore
    classOf[DirectOutputCommitForIterative].asType) {
  this: CacheStrategy =>

  override def defConstructors(ctorDef: ConstructorDef): Unit = {
    ctorDef.newInit(Seq(classOf[Set[IterativeAction[String]]].asType, classOf[JobContext].asType),
      new MethodSignatureBuilder()
        .newParameterType {
          _.newClassType(classOf[Set[_]].asType) {
            _.newTypeArgument(SignatureVisitor.INSTANCEOF) {
              _.newClassType(classOf[IterativeAction[_]].asType) {
                _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[BoxedUnit].asType)
              }
            }
          }
        }
        .newParameterType(classOf[JobContext].asType)
        .newVoidReturnType()) { implicit mb =>

        val thisVar :: preparesVar :: jobContextVar :: _ = mb.argVars

        thisVar.push().invokeInit(
          superType,
          preparesVar.push(),
          jobContextVar.push())
        initMixIns()
      }
  }

  override def defMethods(methodDef: MethodDef): Unit = {
    super.defMethods(methodDef)

    methodDef.newMethod("basePaths", classOf[Set[String]].asType, Seq.empty,
      new MethodSignatureBuilder()
        .newReturnType {
          _.newClassType(classOf[Set[_]].asType) {
            _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[String].asType)
          }
        }) { implicit mb =>
        `return`(
          buildSet { builder =>
            basePaths.foreach { basePath =>
              builder += ldc(basePath)
            }
          })
      }
  }
}
