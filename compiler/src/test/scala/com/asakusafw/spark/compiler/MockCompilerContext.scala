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
package com.asakusafw.spark.compiler

import scala.collection.JavaConversions._
import scala.collection.mutable

import org.objectweb.asm.Type

import com.asakusafw.lang.compiler.api.{ CompilerOptions, DataModelLoader }
import com.asakusafw.lang.compiler.api.JobflowProcessor.{ Context => JPContext }
import com.asakusafw.lang.compiler.api.reference.ExternalInputReference
import com.asakusafw.lang.compiler.api.testing.MockDataModelLoader
import com.asakusafw.lang.compiler.hadoop.{ InputFormatInfo, InputFormatInfoExtension }
import com.asakusafw.lang.compiler.model.description.ClassDescription
import com.asakusafw.lang.compiler.model.info.{ ExternalInputInfo, ExternalOutputInfo }
import com.asakusafw.spark.compiler.graph.{
  BranchKeysClassBuilder,
  BroadcastIdsClassBuilder
}
import com.asakusafw.spark.compiler.spi.{
  AggregationCompiler,
  NodeCompiler,
  OperatorCompiler
}
import com.asakusafw.spark.tools.asm.{ ClassBuilder, SimpleClassLoader }

import resource._

class MockCompilerContext(val flowId: String)
  extends CompilerContext {

  val cl = new SimpleClassLoader(Thread.currentThread.getContextClassLoader)

  def loadClass[C](className: String): Class[C] = cl.loadClass(className).asInstanceOf[Class[C]]

  override def addClass(builder: ClassBuilder): Type = {
    cl.put(builder.thisType.getClassName, builder.build())
    builder.thisType
  }
}

object MockCompilerContext {

  class NodeCompiler(val flowId: String)(jpContext: JPContext)
    extends NodeCompiler.Context
    with OperatorCompiler.Context
    with AggregationCompiler.Context {

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

  class OperatorCompiler(flowId: String)
    extends MockCompilerContext(flowId)
    with OperatorCompiler.Context {

    override val classLoader: ClassLoader = cl
    override val dataModelLoader: DataModelLoader = new MockDataModelLoader(cl)

    override val branchKeys: BranchKeysClassBuilder = new BranchKeysClassBuilder(flowId)
    override val broadcastIds: BroadcastIdsClassBuilder = new BroadcastIdsClassBuilder(flowId)
  }

  class AggregationCompiler(flowId: String)
    extends MockCompilerContext(flowId)
    with AggregationCompiler.Context {

    override val classLoader: ClassLoader = cl
    override val dataModelLoader: DataModelLoader = new MockDataModelLoader(cl)
  }
}
