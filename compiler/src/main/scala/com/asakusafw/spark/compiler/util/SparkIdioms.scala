/*
 * Copyright 2011-2016 Asakusa Framework Team.
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
package util

import org.apache.spark.{ HashPartitioner, Partitioner }
import org.objectweb.asm.Type

import com.asakusafw.spark.compiler.ordering.SortOrderingClassBuilder
import com.asakusafw.spark.runtime.orderings.GroupingOrdering
import com.asakusafw.spark.runtime.rdd.ShuffleKey
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._
import com.asakusafw.spark.tools.asm4s._

object SparkIdioms {

  def partitioner(numPartitions: => Stack)(implicit mb: MethodBuilder): Stack = {
    val partitioner = pushNew(classOf[HashPartitioner].asType)
    partitioner.dup().invokeInit(numPartitions)
    partitioner.asType(classOf[Partitioner].asType)
  }

  def groupingOrdering(implicit mb: MethodBuilder): Stack = {
    pushObject(GroupingOrdering)
      .asType(classOf[Ordering[ShuffleKey]].asType)
  }

  def sortOrdering(
    orderingTypes: Seq[(Type, Boolean)])(
      implicit mb: MethodBuilder,
      context: CompilerContext): Stack = {
    if (orderingTypes.isEmpty) {
      groupingOrdering
    } else {
      pushNew0(SortOrderingClassBuilder.getOrCompile(orderingTypes))
        .asType(classOf[Ordering[ShuffleKey]].asType)
    }
  }
}
