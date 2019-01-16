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
package com.asakusafw.spark.extensions.iterativebatch.compiler
package graph

import org.objectweb.asm.Type

import com.asakusafw.lang.compiler.api.CompilerOptions
import com.asakusafw.lang.compiler.planning.Plan
import com.asakusafw.spark.compiler.{
  ClassLoaderProvider,
  CompilerContext,
  DataModelLoaderProvider
}
import com.asakusafw.spark.compiler.graph.{ BranchKeys, BroadcastIds, Instantiator }
import com.asakusafw.spark.compiler.planning.IterativeInfo
import com.asakusafw.spark.compiler.spi.NodeCompiler

object IterativeJobCompiler {

  def support(plan: Plan): Boolean = {
    IterativeInfo.isIterative(plan)
  }

  def compile(plan: Plan)(implicit context: IterativeJobCompiler.Context): Type = {
    assert(support(plan), s"The plan is not supported.")
    context.addClass(new IterativeJobClassBuilder(plan))
  }

  trait Context
    extends CompilerContext
    with ClassLoaderProvider
    with DataModelLoaderProvider {

    def branchKeys: BranchKeys
    def broadcastIds: BroadcastIds

    def options: CompilerOptions

    def nodeCompilerContext: NodeCompiler.Context
    def instantiatorCompilerContext: Instantiator.Context
  }
}
