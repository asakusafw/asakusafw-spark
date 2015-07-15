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
package com.asakusafw.spark.runtime
package rdd

import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ZippedPartitionsRDDSpecTest extends ZippedPartitionsRDDSpec

class ZippedPartitionsRDDSpec extends FlatSpec with SparkSugar {

  behavior of classOf[ZippedPartitionsRDD[_]].getSimpleName

  it should "zip partitions" in {
    val rdd1 = sc.parallelize(0 until 100, 4)
    val rdd2 = sc.parallelize(100 until 200, 4)
    val rdd3 = sc.parallelize(200 until 300, 4)
    val zipped = zipPartitions(Seq(rdd1, rdd2, rdd3)) {
      case Seq(
        iter1: Iterator[Int] @unchecked,
        iter2: Iterator[Int] @unchecked,
        iter3: Iterator[Int] @unchecked) =>
        iter1 ++ iter2 ++ iter3
    }
    assert(zipped.collect ===
      (for {
        p <- 0 until 4
        h <- 0 until 3
        i <- 0 until 25
      } yield {
        i + (h * 100) + (p * 25)
      }))
  }
}
