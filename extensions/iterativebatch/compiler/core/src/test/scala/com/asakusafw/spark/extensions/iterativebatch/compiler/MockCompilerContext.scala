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

import scala.collection.JavaConversions._
import scala.collection.mutable

import org.objectweb.asm.Type

import com.asakusafw.lang.compiler.api.{ CompilerOptions, DataModelLoader }
import com.asakusafw.lang.compiler.api.JobflowProcessor.{ Context => JPContext }
import com.asakusafw.lang.compiler.api.reference.ExternalInputReference
import com.asakusafw.lang.compiler.hadoop.{ InputFormatInfo, InputFormatInfoExtension }
import com.asakusafw.lang.compiler.model.description.ClassDescription
import com.asakusafw.lang.compiler.model.info.{ ExternalInputInfo, ExternalOutputInfo }
import com.asakusafw.spark.compiler.graph.{
  BranchKeysClassBuilder,
  BroadcastIdsClassBuilder,
  Instantiator
}
import com.asakusafw.spark.compiler.spi.{ AggregationCompiler, NodeCompiler, OperatorCompiler }
import com.asakusafw.spark.tools.asm.ClassBuilder

import com.asakusafw.spark.extensions.iterativebatch.compiler.graph.IterativeJobCompiler

import resource._

object MockCompilerContext {

  class IterativeJobCompiler(val flowId: String)(jpContext: JPContext)
    extends IterativeJobCompiler.Context
    with NodeCompiler.Context
    with Instantiator.Context
    with OperatorCompiler.Context
    with AggregationCompiler.Context {

    override def nodeCompilerContext: NodeCompiler.Context = this
    override def instantiatorCompilerContext: Instantiator.Context = this

    override def operatorCompilerContext: OperatorCompiler.Context = this
    override def aggregationCompilerContext: AggregationCompiler.Context = this

    override def classLoader: ClassLoader = jpContext.getClassLoader
    override def dataModelLoader: DataModelLoader = jpContext.getDataModelLoader
    override def options: CompilerOptions = jpContext.getOptions

    override val branchKeys: BranchKeysClassBuilder = new BranchKeysClassBuilder(flowId)
    override val broadcastIds: BroadcastIdsClassBuilder = new BroadcastIdsClassBuilder(flowId)

    override def getInputFormatInfo(
      name: String, info: ExternalInputInfo): Option[InputFormatInfo] = {
      Option(InputFormatInfoExtension.resolve(jpContext, name, info))
    }

    private val externalInputs: mutable.Map[String, ExternalInputReference] = mutable.Map.empty

    override def addExternalInput(
      name: String, info: ExternalInputInfo): ExternalInputReference = {
      externalInputs.getOrElseUpdate(
        name,
        jpContext.addExternalInput(name, info))
    }

    private val externalOutputs: mutable.Map[String, Unit] = mutable.Map.empty

    override def addExternalOutput(
      name: String, info: ExternalOutputInfo, paths: Seq[String]): Unit = {
      externalOutputs.getOrElseUpdate(
        name,
        jpContext.addExternalOutput(name, info, paths))
    }

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
