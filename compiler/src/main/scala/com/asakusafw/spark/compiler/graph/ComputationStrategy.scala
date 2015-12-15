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
package graph

import scala.concurrent.ExecutionContext

import org.objectweb.asm.Opcodes

import com.asakusafw.spark.runtime.RoundContext
import com.asakusafw.spark.runtime.graph.{ ComputeOnce => ComputeOnceTrait }
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm4s._
import com.asakusafw.spark.tools.asm4s.MixIn._

trait ComputationStrategy extends ClassBuilder with Mixing

trait ComputeOnce extends ComputationStrategy {

  override val mixins = Seq(
    MixIn(classOf[ComputeOnceTrait].asType,
      Seq(FieldDef(Opcodes.ACC_TRANSIENT, "generatedRDD", classOf[Map[_, _]].asType)),
      Seq(
        MethodDef("getOrCompute",
          classOf[Map[_, _]].asType,
          classOf[RoundContext].asType,
          classOf[ExecutionContext].asType))))
}
