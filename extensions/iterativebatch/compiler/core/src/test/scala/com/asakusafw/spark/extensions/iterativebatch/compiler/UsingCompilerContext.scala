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
package com.asakusafw.spark.extensions.iterativebatch.compiler

import java.io.File

import scala.collection.JavaConversions._

import com.asakusafw.lang.compiler.api.CompilerOptions
import com.asakusafw.lang.compiler.api.JobflowProcessor.{ Context => JPContext }
import com.asakusafw.lang.compiler.api.testing.MockJobflowProcessorContext

import com.asakusafw.spark.compiler.{ UsingCompilerContext => UCC }

trait UsingCompilerContext extends UCC {

  def newIterativeJobCompilerContext(
    flowId: String, outputDir: File): MockCompilerContext.IterativeJobCompiler = {
    newIterativeJobCompilerContext(
      flowId,
      new MockJobflowProcessorContext(
        new CompilerOptions("buildid", "", Map.empty[String, String]),
        Thread.currentThread.getContextClassLoader,
        outputDir))
  }

  def newIterativeJobCompilerContext(
    flowId: String,
    jpContext: JPContext): MockCompilerContext.IterativeJobCompiler = {
    new MockCompilerContext.IterativeJobCompiler(flowId)(jpContext)
  }
}
