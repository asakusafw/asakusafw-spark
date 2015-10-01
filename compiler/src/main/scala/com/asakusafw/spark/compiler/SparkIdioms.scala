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

import org.apache.spark.{ HashPartitioner, Partitioner }
import org.objectweb.asm.Type

import com.asakusafw.spark.compiler.ordering.{
  GroupingOrderingClassBuilder,
  SortOrderingClassBuilder
}
import com.asakusafw.spark.runtime.driver.ShuffleKey
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._

trait SparkIdioms extends ScalaIdioms {

  def partitioner(mb: MethodBuilder)(numPartitions: => Stack): Stack = {
    import mb._ // scalastyle:ignore
    val partitioner = pushNew(classOf[HashPartitioner].asType)
    partitioner.dup().invokeInit(numPartitions)
    partitioner.asType(classOf[Partitioner].asType)
  }

  def groupingOrdering(mb: MethodBuilder)(
    groupingTypes: Seq[Type])(
      implicit context: CompilerContext): Stack = {
    import mb._ // scalastyle:ignore
    pushNew0(GroupingOrderingClassBuilder.getOrCompile(groupingTypes))
      .asType(classOf[Ordering[ShuffleKey]].asType)
  }

  def sortOrdering(mb: MethodBuilder)(
    groupingTypes: Seq[Type],
    orderingTypes: Seq[(Type, Boolean)])(
      implicit context: CompilerContext): Stack = {
    import mb._ // scalastyle:ignore
    pushNew0(SortOrderingClassBuilder.getOrCompile(groupingTypes, orderingTypes))
      .asType(classOf[Ordering[ShuffleKey]].asType)
  }
}
