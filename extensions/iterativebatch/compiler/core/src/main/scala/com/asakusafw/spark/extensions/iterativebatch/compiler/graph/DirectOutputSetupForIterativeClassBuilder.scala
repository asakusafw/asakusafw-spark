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

import org.objectweb.asm.Type

import com.asakusafw.spark.compiler._
import com.asakusafw.spark.compiler.graph.CacheStrategy
import com.asakusafw.spark.runtime.JobContext
import com.asakusafw.spark.runtime.graph.DirectOutputSetup
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._

import com.asakusafw.spark.extensions.iterativebatch.runtime.graph.DirectOutputSetupForIterative

abstract class DirectOutputSetupForIterativeClassBuilder(
  setupType: Type)(
    implicit val context: CompilerContext)
  extends ClassBuilder(
    Type.getType(
      s"L${GeneratedClassPackageInternalName}/${context.flowId}/graph/DirectOutputSetupForIterative;"), // scalastyle:ignore
    classOf[DirectOutputSetupForIterative].asType) {
  self: CacheStrategy =>

  override def defConstructors(ctorDef: ConstructorDef): Unit = {
    ctorDef.newInit(Seq(classOf[JobContext].asType)) { implicit mb =>

      val thisVar :: jobContextVar :: _ = mb.argVars

      thisVar.push().invokeInit(
        superType,
        {
          val setup = pushNew(setupType)
          setup.dup().invokeInit(jobContextVar.push())
          setup.asType(classOf[DirectOutputSetup].asType)
        },
        jobContextVar.push())
      initMixIns()
    }
  }
}
