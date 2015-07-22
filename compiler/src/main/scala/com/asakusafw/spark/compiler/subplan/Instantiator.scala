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
      vars: Instantiator.Vars,
      nextLocal: AtomicInteger)(
        implicit context: SparkClientCompiler.Context): Var
}

object Instantiator {

  case class Vars(
    sc: Var, // SparkContext
    hadoopConf: Var, // Broadcast[Configuration]
    rdds: Var, // mutable.Map[BranchKey, Future[RDD[_]]]
    terminators: Var, // mutable.Set[Future[Unit]]
    broadcasts: Var // Map[BroadcastId, Future[Broadcast[Map[ShuffleKey, Seq[_]]]]]
    )
}
