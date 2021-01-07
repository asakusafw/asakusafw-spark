/*
 * Copyright 2011-2021 Asakusa Framework Team.
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
package branching

import com.asakusafw.spark.compiler.spi.AggregationCompiler
import com.asakusafw.spark.tools.asm.ClassBuilder

trait Branching
  extends ClassBuilder
  with BranchKeysField
  with PartitionersField
  with OrderingsField
  with Aggregations
  with PreparingKey
  with Deserializer {

  override def context: Branching.Context
}

object Branching {

  trait Context
    extends CompilerContext
    with BranchKeysField.Context
    with PartitionersField.Context
    with OrderingsField.Context
    with Aggregations.Context
    with PreparingKey.Context
    with Deserializer.Context
}
