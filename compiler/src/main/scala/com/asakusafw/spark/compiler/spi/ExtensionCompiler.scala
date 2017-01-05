/*
 * Copyright 2011-2017 Asakusa Framework Team.
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
package com.asakusafw.spark.compiler.spi

import java.util.ServiceLoader

import scala.collection.JavaConversions._
import scala.collection.mutable

import com.asakusafw.lang.compiler.api.JobflowProcessor.{ Context => JPContext }
import com.asakusafw.lang.compiler.planning.Plan

trait ExtensionCompiler {

  def support(plan: Plan)(jpContext: JPContext): Boolean

  def compile(plan: Plan)(flowId: String, jpContext: JPContext): Unit
}

object ExtensionCompiler {

  def find(plan: Plan)(jpContext: JPContext): Option[ExtensionCompiler] = {
    apply(jpContext.getClassLoader).find(_.support(plan)(jpContext))
  }

  private[this] val extensions: mutable.Map[ClassLoader, Seq[ExtensionCompiler]] =
    mutable.WeakHashMap.empty

  private[this] def apply(
    classLoader: ClassLoader): Seq[ExtensionCompiler] = {
    extensions.getOrElse(classLoader, reload(classLoader))
  }

  private[this] def reload(
    classLoader: ClassLoader): Seq[ExtensionCompiler] = {
    val ors = ServiceLoader.load(classOf[ExtensionCompiler], classLoader).toSeq
    extensions(classLoader) = ors
    ors
  }
}
