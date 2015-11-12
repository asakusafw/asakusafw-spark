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
package com.asakusafw.spark.extensions.iterativebatch.compiler.flow

import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.reflect.ClassTag

import org.objectweb.asm.{ Opcodes, Type }

import com.asakusafw.spark.runtime.rdd.BranchKey
import com.asakusafw.spark.tools.asm._

import com.asakusafw.spark.extensions.iterativebatch.compiler.util.MixIn
import com.asakusafw.spark.extensions.iterativebatch.compiler.util.MixIn._
import com.asakusafw.spark.extensions.iterativebatch.runtime.RoundContext
import com.asakusafw.spark.extensions.iterativebatch.runtime.flow.{
  ComputeAlways,
  ComputeOnce,
  Source
}

object ComputeStrategy {

  val ComputeAlways: MixIn = mixInComputeStrategy(classOf[ComputeAlways].asType)
  val ComputeOnce: MixIn = mixInComputeStrategy(classOf[ComputeOnce].asType)

  private def mixInComputeStrategy(traitType: Type): MixIn =
    MixIn(traitType,
      Seq(FieldDef(Opcodes.ACC_TRANSIENT, "generatedRDDs", classOf[mutable.Map[_, _]].asType)),
      Seq(
        MethodDef("getOrCompute",
          classOf[Map[_, _]].asType,
          classOf[RoundContext].asType,
          classOf[ExecutionContext].asType),
        MethodDef("map",
          classOf[Source].asType,
          classOf[BranchKey].asType,
          classOf[_ => _].asType,
          classOf[ClassTag[_]].asType),
        MethodDef("mapWithRoundContext",
          classOf[Source].asType,
          classOf[BranchKey].asType,
          classOf[_ => _ => _].asType,
          classOf[ClassTag[_]].asType),
        MethodDef("flatMap",
          classOf[Source].asType,
          classOf[BranchKey].asType,
          classOf[_ => _].asType,
          classOf[ClassTag[_]].asType),
        MethodDef("flatMapWithRoundContext",
          classOf[Source].asType,
          classOf[BranchKey].asType,
          classOf[_ => _ => _].asType,
          classOf[ClassTag[_]].asType),
        MethodDef("mapPartitions",
          classOf[Source].asType,
          classOf[BranchKey].asType,
          classOf[_ => _].asType,
          Type.BOOLEAN_TYPE,
          classOf[ClassTag[_]].asType),
        MethodDef("mapPartitionsWithRoundContext",
          classOf[Source].asType,
          classOf[BranchKey].asType,
          classOf[_ => _ => _].asType,
          Type.BOOLEAN_TYPE,
          classOf[ClassTag[_]].asType),
        MethodDef("mapPartitionsWithIndex",
          classOf[Source].asType,
          classOf[BranchKey].asType,
          classOf[(_, _) => _].asType,
          Type.BOOLEAN_TYPE,
          classOf[ClassTag[_]].asType),
        MethodDef("mapPartitionsWithIndexAndRoundContext",
          classOf[Source].asType,
          classOf[BranchKey].asType,
          classOf[_ => (_, _) => _].asType,
          Type.BOOLEAN_TYPE,
          classOf[ClassTag[_]].asType)))
}
