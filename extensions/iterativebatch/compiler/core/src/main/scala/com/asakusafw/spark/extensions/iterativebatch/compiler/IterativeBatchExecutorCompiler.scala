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

import org.objectweb.asm.Type

import com.asakusafw.lang.compiler.planning.Plan
import com.asakusafw.spark.compiler.{
  ClassLoaderProvider,
  CompilerContext,
  DataModelLoaderProvider
}
import com.asakusafw.spark.compiler.graph.{ BranchKeys, BroadcastIds, Instantiator }
import com.asakusafw.spark.compiler.spi.NodeCompiler

object IterativeBatchExecutorCompiler {

  def compile(plan: Plan)(implicit context: IterativeBatchExecutorCompiler.Context): Type = {
    context.addClass(new IterativeBatchExecutorClassBuilder(plan))
  }

  trait Context
    extends CompilerContext
    with ClassLoaderProvider
    with DataModelLoaderProvider {

    def branchKeys: BranchKeys
    def broadcastIds: BroadcastIds

    def nodeCompilerContext: NodeCompiler.Context
    def instantiatorCompilerContext: Instantiator.Context
  }
}
