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
package com.asakusafw.yass.runtime
package flow

import scala.concurrent.Future
import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD

import com.asakusafw.spark.runtime.rdd.BranchKey

class ParallelCollectionSource[T: ClassTag](
  val branchKey: BranchKey,
  data: Seq[T],
  numSlices: Option[Int] = None)(
    val label: String)
  extends Source {

  @transient
  private var result: Future[RDD[T]] = _

  override def compute(rc: RoundContext): Map[BranchKey, Future[RDD[_]]] = synchronized {
    if (result == null) {
      val rdd = rc.sc.parallelize(data, numSlices.getOrElse(rc.sc.defaultParallelism))
      result = Future.successful(rdd)
    }
    Map(branchKey -> result)
  }
}
