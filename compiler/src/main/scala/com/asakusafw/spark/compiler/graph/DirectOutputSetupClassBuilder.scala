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

import org.objectweb.asm.Type
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.spark.runtime.JobContext
import com.asakusafw.spark.runtime.graph.DirectOutputSetup
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._
import com.asakusafw.spark.tools.asm4s._

abstract class DirectOutputSetupClassBuilder(
  specs: Set[(String, String, Seq[String])])(
    implicit val context: CompilerContext)
  extends ClassBuilder(
    Type.getType(
      s"L${GeneratedClassPackageInternalName}/${context.flowId}/graph/DirectOutputSetup;"),
    classOf[DirectOutputSetup].asType) {
  self: CacheStrategy =>

  override def defConstructors(ctorDef: ConstructorDef): Unit = {
    ctorDef.newInit(Seq(classOf[JobContext].asType)) { implicit mb =>

      val thisVar :: jobContextVar :: _ = mb.argVars

      thisVar.push().invokeInit(
        superType,
        jobContextVar.push())
      initMixIns()
    }
  }

  override def defMethods(methodDef: MethodDef): Unit = {
    super.defMethods(methodDef)

    methodDef.newMethod("specs", classOf[Set[(String, String, Seq[String])]].asType, Seq.empty,
      new MethodSignatureBuilder()
        .newReturnType {
          _.newClassType(classOf[Set[_]].asType) {
            _.newTypeArgument(SignatureVisitor.INSTANCEOF) {
              _.newClassType(classOf[(_, _, _)].asType) {
                _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[String].asType)
                  .newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[String].asType)
                  .newTypeArgument(SignatureVisitor.INSTANCEOF) {
                    _.newClassType(classOf[Seq[_]].asType) {
                      _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[String].asType)
                    }
                  }
              }
            }
          }
        }) { implicit mb =>
        `return`(
          buildSet { builder =>
            for {
              (id, basePath, deletePatterns) <- specs
            } {
              builder +=
                tuple3(
                  ldc(id),
                  ldc(basePath),
                  buildSeq { builder =>
                    deletePatterns.foreach { deletePattern =>
                      builder += ldc(deletePattern)
                    }
                  })
            }
          })
      }
  }
}
