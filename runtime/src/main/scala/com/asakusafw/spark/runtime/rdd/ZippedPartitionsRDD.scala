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
package com.asakusafw.spark.runtime.rdd

import scala.reflect.ClassTag

import org.apache.spark._
import org.apache.spark.rdd.RDD

import org.apache.spark.rdd.backdoor._

class ZippedPartitionsRDD[V: ClassTag](
  sc: SparkContext,
  var f: (Seq[Iterator[_]]) => Iterator[V],
  var _rdds: Seq[RDD[_]],
  preservesPartitioning: Boolean = false)
  extends ZippedPartitionsBaseRDD[V](sc, _rdds, preservesPartitioning) {

  override def compute(s: Partition, context: TaskContext): Iterator[V] = {
    val partitions = s.asInstanceOf[ZippedPartitionsPartition].partitions
    f(rdds.zipWithIndex.map {
      case (rdd, i) => rdd.iterator(partitions(i), context)
    })
  }

  override def clearDependencies(): Unit = {
    super.clearDependencies()
    _rdds = null // scalastyle:ignore
    f = null // scalastyle:ignore
  }
}
