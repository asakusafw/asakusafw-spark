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

import scala.collection.mutable
import scala.collection.JavaConversions._

import org.objectweb.asm.Type

import com.asakusafw.lang.compiler.api.{ CompilerOptions, DataModelLoader }
import com.asakusafw.lang.compiler.api.JobflowProcessor.{ Context => JPContext }
import com.asakusafw.lang.compiler.model.description.ClassDescription
import com.asakusafw.spark.compiler.spi.{ AggregationCompiler, OperatorCompiler }
import com.asakusafw.spark.compiler.subplan.{
  BranchKeysClassBuilder,
  BroadcastIdsClassBuilder
}
import com.asakusafw.spark.tools.asm.{ ClassBuilder, SimpleClassLoader }

import com.asakusafw.spark.extensions.iterativebatch.compiler.spi.NodeCompiler

import resource._

object MockCompilerContext {

  class NodeCompiler(val flowId: String)(jpContext: JPContext)
    extends NodeCompiler.Context
    with OperatorCompiler.Context
    with AggregationCompiler.Context {

    override def operatorCompilerContext: OperatorCompiler.Context = this
    override def aggregationCompilerContext: AggregationCompiler.Context = this

    override def classLoader: ClassLoader = jpContext.getClassLoader
    override def dataModelLoader: DataModelLoader = jpContext.getDataModelLoader

    override val branchKeys: BranchKeysClassBuilder = new BranchKeysClassBuilder(flowId)
    override val broadcastIds: BroadcastIdsClassBuilder = new BroadcastIdsClassBuilder(flowId)

    override def addClass(builder: ClassBuilder): Type = {
      for {
        os <- managed(jpContext.addClassFile(new ClassDescription(builder.thisType.getClassName)))
      } {
        os.write(builder.build())
      }
      builder.thisType
    }
  }
}
