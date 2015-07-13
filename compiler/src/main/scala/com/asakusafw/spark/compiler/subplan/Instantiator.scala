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
package subplan

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable

import org.objectweb.asm.Type

import com.asakusafw.lang.compiler.api.JobflowProcessor.{ Context => JPContext }
import com.asakusafw.lang.compiler.planning._
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._

trait Instantiator {

  def newInstance(
    driverType: Type,
    subplan: SubPlan)(
      mb: MethodBuilder,
      scVar: Var, // SparkContext
      hadoopConfVar: Var, // Broadcast[Configuration]
      broadcastsVar: Var, // Map[BroadcastId, Broadcast[Map[ShuffleKey, Seq[_]]]]
      rddsVar: Var, // mutable.Map[BranchKey, RDD[_]]
      terminatorsVar: Var, // mutable.Set[Future[Unit]]
      nextLocal: AtomicInteger)(
        implicit context: SparkClientCompiler.Context): Var
}
