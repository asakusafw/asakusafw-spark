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
package com.asakusafw.spark.runtime
package rdd

import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner

import org.apache.spark.Partitioner
import org.apache.spark.rdd.{ RDD, ShuffledRDD }

@RunWith(classOf[JUnitRunner])
class SortMergeCoGroupSpecTest extends SortMergeCoGroupSpec

class SortMergeCoGroupSpec extends FlatSpec with SparkForAll {

  import SortMergeCoGroupSpec._

  behavior of "SortMergeCoGroup"

  it should "smcogroup rdds" in {
    val rdd1 = sc.parallelize(0 until 100).map(i =>
      ((i.toString, 10), i.toString))
    val rdd2 = sc.parallelize(0 until 100).flatMap(i => Seq(
      ((i.toString, 10), i * 10),
      ((i.toString, 0), i)))

    val part = new GroupingPartitioner(2)
    val ord3 = Ordering.Tuple2(implicitly[Ordering[String]], implicitly[Ordering[Int]].reverse)
    val rdd3: RDD[((String, Int), Long)] =
      sc.parallelize(0 until 100).flatMap(i => Seq(
        ((i.toString, 0), i.toLong),
        ((i.toString, 10), i.toLong * 10)))
        .shuffle(part, Option(ord3))

    val grouping = new GroupingOrdering
    val cogrouped = sc.smcogroup(
      Seq(
        (rdd1.asInstanceOf[RDD[((String, Int), Any)]], None),
        (rdd2.asInstanceOf[RDD[((String, Int), Any)]], Some(implicitly[Ordering[(String, Int)]])),
        (rdd3.asInstanceOf[RDD[((String, Int), Any)]], Some(ord3))),
      part,
      grouping)
    val (part0, part1) = (0 until 100).map(_.toString).sorted.partition { k =>
      val part = k.hashCode % 2
      (if (part < 0) part + 2 else part) == 0
    }
    cogrouped.mapValues(_.map(_.toArray)).collect.zip((part0 ++ part1).map(k => (k, 0))).foreach {
      case ((actualKey, actualValues), key @ (k, _)) =>
        assert(grouping.compare(actualKey, key) === 0)
        assert(actualValues.size === 3)
        assert(actualValues(0) === Seq(k))
        assert(actualValues(1) === Seq(k.toInt, k.toInt * 10))
        assert(actualValues(2) === Seq(k.toLong * 10, k.toLong))
    }
  }

  it should "smcogroup 1 rdd" in {
    val rdd = sc.parallelize(0 until 100).map(i =>
      ((i.toString, 10), i.toString))

    val part = new GroupingPartitioner(2)
    val grouping = new GroupingOrdering
    val cogrouped = sc.smcogroup(
      Seq((rdd.asInstanceOf[RDD[((String, Int), Any)]], None)),
      part,
      grouping)
    val (part0, part1) = (0 until 100).map(_.toString).sorted.partition { k =>
      val part = k.hashCode % 2
      (if (part < 0) part + 2 else part) == 0
    }
    cogrouped.mapValues(_.map(_.toArray)).collect.zip((part0 ++ part1).map(k => (k, 0))).foreach {
      case ((actualKey, actualValues), key @ (k, _)) =>
        assert(grouping.compare(actualKey, key) === 0)
        assert(actualValues.size === 1)
        assert(actualValues(0) === Seq(k))
    }
  }

  it should "smcogroup empty rdds" in {
    val part = new GroupingPartitioner(2)
    val grouping = new GroupingOrdering
    val cogrouped = sc.smcogroup(
      Seq.empty,
      part,
      grouping)
    assert(cogrouped.collect === Array.empty)
  }
}

object SortMergeCoGroupSpec {

  class GroupingPartitioner(val numPartitions: Int) extends Partitioner {

    override def getPartition(key: Any): Int = {
      val (group, _) = key.asInstanceOf[(String, Int)]
      val part = group.hashCode % numPartitions
      if (part < 0) part + numPartitions else part
    }
  }

  class GroupingOrdering extends Ordering[(String, Int)] {

    override def compare(x: (String, Int), y: (String, Int)): Int = {
      implicitly[Ordering[String]].compare(x._1, y._1)
    }
  }
}
