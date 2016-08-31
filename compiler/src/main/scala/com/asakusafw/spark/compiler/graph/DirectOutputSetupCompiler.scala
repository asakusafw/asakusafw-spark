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
package com.asakusafw.spark.compiler
package graph

import scala.collection.JavaConversions._

import org.objectweb.asm.Type

import com.asakusafw.lang.compiler.extension.directio.DirectFileIoModels
import com.asakusafw.lang.compiler.model.graph.ExternalOutput

object DirectOutputSetupCompiler {

  def compile(
    outputs: Set[ExternalOutput])(
      implicit context: CompilerContext): Type = {
    assert(outputs.nonEmpty)
    outputs.foreach { output =>
      assert(DirectFileIoModels.isSupported(output.getInfo),
        s"The output is not supported: ${output}")
    }

    val builder = new DirectOutputSetupClassBuilder(
      outputs.map { output =>
        val model = DirectFileIoModels.resolve(output.getInfo)
        (output.getName, model.getBasePath, model.getDeletePatterns.toSeq)
      }) with CacheOnce

    context.addClass(builder)
  }
}
