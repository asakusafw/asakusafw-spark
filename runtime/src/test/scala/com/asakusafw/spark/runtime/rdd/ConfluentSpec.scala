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

import com.asakusafw.runtime.value.IntOption

@RunWith(classOf[JUnitRunner])
class ConfluentSpecTest extends ConfluentSpec

class ConfluentSpec extends FlatSpec with SparkForAll {

  import ConfluentSpec._

  behavior of "Confluent"

  it should "confluent rdds" in {
    val rdd1 = sc.parallelize(0 until 100).map(i => ((i.toString, 0), i))
    val rdd2 = sc.parallelize(0 until 100).flatMap(i => Seq(((i.toString, 1), i + 100), ((i.toString, 2), i + 200)))

    val part = new GroupingPartitioner(2)
    val ord = implicitly[Ordering[(String, Int)]]
    val rdd3: RDD[((String, Int), Int)] =
      sc.parallelize(0 until 100).flatMap(i => Seq(((i.toString, 4), i + 400), ((i.toString, 3), i + 300)))
        .shuffle(part, Option(ord))

    val confluented = sc.confluent(Seq(rdd1, rdd2, rdd3), part, Some(ord))
    val (part0, part1) = (0 until 100).sortBy(_.toString).partition { i =>
      val part = i.toString.hashCode % 2
      (if (part < 0) part + 2 else part) == 0
    }
    assert(confluented.collect ===
      (part0 ++ part1).flatMap(i => (0 until 5).map(j => ((i.toString, j), i + 100 * j))))
  }

  it should "confluent empty rdds" in {
    val part = new GroupingPartitioner(2)
    val ord = implicitly[Ordering[(String, Int)]]

    val confluented = sc.confluent(Seq.empty, part, Some(ord))
    assert(confluented.collect === Array.empty)
  }

  it should "confluent rdds of mutable values" in {
    val part = new GroupingPartitioner(2)
    val ord = implicitly[Ordering[(String, Int)]]

    val rdd1: RDD[((String, Int), IntOption)] =
      sc.parallelize(0 until 10).map(i => ((i.toString, 0), i))
        .shuffle(part, Option(ord))
        .mapPartitions(f, preservesPartitioning = true)

    val rdd2: RDD[((String, Int), IntOption)] =
      sc.parallelize(0 until 10).flatMap(i => Seq(((i.toString, 1), i + 10), ((i.toString, 2), i + 20)))
        .shuffle(part, Option(ord))
        .mapPartitions(f, preservesPartitioning = true)

    val rdd3: RDD[((String, Int), IntOption)] =
      sc.parallelize(0 until 10).flatMap(i => Seq(((i.toString, 4), i + 40), ((i.toString, 3), i + 30)))
        .shuffle(part, Option(ord))
        .mapPartitions(f, preservesPartitioning = true)

    val confluented = sc.confluent(Seq(rdd1, rdd2, rdd3), part, Some(ord))
    val (part0, part1) = (0 until 10).sortBy(_.toString).partition { i =>
      val part = i.toString.hashCode % 2
      (if (part < 0) part + 2 else part) == 0
    }
    assert(confluented.map {
      case (k, v) => k -> v.get
    }.collect ===
      (part0 ++ part1).flatMap(i => (0 until 5).map(j => ((i.toString, j), i + 10 * j))))
  }
}

object ConfluentSpec {

  class GroupingPartitioner(val numPartitions: Int) extends Partitioner {

    override def getPartition(key: Any): Int = {
      val (group, _) = key.asInstanceOf[(String, Int)]
      val part = group.hashCode % numPartitions
      if (part < 0) part + numPartitions else part
    }
  }

  def f: Iterator[((String, Int), Int)] => Iterator[((String, Int), IntOption)] = {

    lazy val intOption = new IntOption()

    {
      _.map { value =>
        intOption.modify(value._2)
        (value._1, intOption)
      }
    }
  }
}
