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
package com.asakusafw.spark.extensions.iterativebatch.compiler.spi

import java.util.ServiceLoader

import scala.collection.JavaConversions._
import scala.collection.mutable

import org.objectweb.asm.Type

import com.asakusafw.lang.compiler.api.CompilerOptions
import com.asakusafw.lang.compiler.api.reference.ExternalInputReference
import com.asakusafw.lang.compiler.hadoop.InputFormatInfo
import com.asakusafw.lang.compiler.model.info.{ ExternalInputInfo, ExternalOutputInfo }
import com.asakusafw.lang.compiler.planning.SubPlan
import com.asakusafw.spark.compiler.{
  ClassLoaderProvider,
  CompilerContext,
  DataModelLoaderProvider
}
import com.asakusafw.spark.compiler.graph.{ BranchKeys, Instantiator }
import com.asakusafw.spark.compiler.graph.branching.Branching
import com.asakusafw.spark.compiler.spi.{ AggregationCompiler, NodeCompiler, OperatorCompiler }

trait RoundAwareNodeCompiler {

  def support(
    subplan: SubPlan)(
      implicit context: NodeCompiler.Context): Boolean

  def instantiator: Instantiator

  def compile(
    subplan: SubPlan)(
      implicit context: NodeCompiler.Context): Type
}

object RoundAwareNodeCompiler {

  def get(subplan: SubPlan)(
    implicit context: NodeCompiler.Context): RoundAwareNodeCompiler = {
    val compilers = apply(context.classLoader).filter(_.support(subplan))
    assert(compilers.size != 0,
      s"The compiler supporting subplan (${subplan}) is not found.")
    assert(compilers.size == 1,
      "The number of compiler supporting subplan "
        + s"(${subplan}) should be 1: ${compilers.size}")
    compilers.head
  }

  private[this] val nodeCompilers: mutable.Map[ClassLoader, Seq[RoundAwareNodeCompiler]] = // scalastyle:ignore
    mutable.WeakHashMap.empty

  private[this] def apply(
    classLoader: ClassLoader): Seq[RoundAwareNodeCompiler] = {
    nodeCompilers.getOrElse(classLoader, reload(classLoader))
  }

  private[this] def reload(
    classLoader: ClassLoader): Seq[RoundAwareNodeCompiler] = {
    val ors = ServiceLoader.load(classOf[RoundAwareNodeCompiler], classLoader).toSeq
    nodeCompilers(classLoader) = ors
    ors
  }
}
