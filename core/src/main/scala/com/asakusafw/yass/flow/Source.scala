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
package com.asakusafw.yass
package flow

import scala.collection.mutable
import scala.concurrent.Future
import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD

import com.asakusafw.spark.runtime.driver.ShuffleKey
import com.asakusafw.spark.runtime.rdd.BranchKey

trait Source extends Node {

  def compute(rc: RoundContext): Map[BranchKey, Future[RDD[_]]]

  final def getOrCompute(rc: RoundContext): Map[BranchKey, Future[RDD[_]]] = {
    Source.getOrCompute(this)(rc)
  }
}

object Source {

  private[this] val generatedRDDs =
    mutable.WeakHashMap.empty[Source, mutable.Map[RoundContext, Map[BranchKey, Future[RDD[_]]]]]

  private def getOrCompute(source: Source)(rc: RoundContext): Map[BranchKey, Future[RDD[_]]] =
    synchronized {
      generatedRDDs
        .getOrElseUpdate(source, mutable.WeakHashMap.empty)
        .getOrElseUpdate(rc, source.compute(rc))
    }
}
