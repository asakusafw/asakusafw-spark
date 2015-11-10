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
package flow

import java.util.concurrent.atomic.AtomicInteger

import org.objectweb.asm.Type

import com.asakusafw.lang.compiler.planning.SubPlan
import com.asakusafw.spark.compiler.{
  ClassLoaderProvider,
  CompilerContext,
  DataModelLoaderProvider
}
import com.asakusafw.spark.compiler.subplan.BranchKeys
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._

trait Instantiator {

  def newInstance(
    nodeType: Type,
    subplan: SubPlan,
    subplanToIdx: Map[SubPlan, Int])(
      mb: MethodBuilder,
      vars: Instantiator.Vars,
      nextLocal: AtomicInteger)(
        implicit context: Instantiator.Context): Var
}

object Instantiator {

  trait Context
    extends CompilerContext
    with ClassLoaderProvider
    with DataModelLoaderProvider {

    def branchKeys: BranchKeys
  }

  case class Vars(
    sc: Var, // SparkContext
    nodes: Var, // Array[Node]
    broadcasts: Var // Map[BroadcastId, Broadcast]
    )
}
