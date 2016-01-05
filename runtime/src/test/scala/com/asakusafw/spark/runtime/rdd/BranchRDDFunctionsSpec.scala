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
package com.asakusafw.spark.runtime
package rdd

import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner

import org.apache.spark.{ HashPartitioner, TaskContext }

@RunWith(classOf[JUnitRunner])
class BranchRDDFunctionsSpecTest extends BranchRDDFunctionsSpec

class BranchRDDFunctionsSpec extends FlatSpec with SparkForAll {

  behavior of "BranchRDDFunctions"

  it should "branch by odd or even" in {
    val rdd = sc.parallelize(0 until 100)
    val branched = rdd.branch(Set(BranchKey(0), BranchKey(1)), iter => {
      iter.map(i => (Branch(BranchKey(if (i % 2 == 0) 1 else 0), null), (i, TaskContext.get.partitionId)))
    })

    assert(branched(BranchKey(1)).map(_._2._1).collect.toSeq === (0 until 100 by 2))
    assert(branched(BranchKey(1)).partitioner.isEmpty)
    assert(branched(BranchKey(1)).partitions.length === rdd.partitions.length)
    assert(branched(BranchKey(1)).map {
      case (_, (_, partid)) => partid == TaskContext.get.partitionId
    }.collect.forall(_ == true))

    assert(branched(BranchKey(0)).map(_._2._1).collect.toSeq === (1 until 100 by 2))
    assert(branched(BranchKey(0)).partitioner.isEmpty)
    assert(branched(BranchKey(0)).partitions.length === rdd.partitions.length)
    assert(branched(BranchKey(0)).map {
      case (_, (_, partid)) => partid == TaskContext.get.partitionId
    }.collect.forall(_ == true))
  }

  it should "branch and shuffle by odd or even" in {
    val rdd = sc.parallelize(0 until 100)
    val branched = rdd.branch(Set(BranchKey(0), BranchKey(1)), iter => {
      iter.map(i => (Branch(BranchKey(if (i % 2 == 0) 1 else 0), i), (i, TaskContext.get.partitionId)))
    },
      partitioners = Map(BranchKey(1) -> new HashPartitioner(3)))

    assert(branched(BranchKey(1)).map(_._2._1).collect.toSeq ===
      rdd.filter(_ % 2 == 0).map(i => (i, i)).partitionBy(new HashPartitioner(3)).map(_._2).collect.toSeq)
    assert(branched(BranchKey(1)).partitioner.isDefined)
    assert(branched(BranchKey(1)).partitions.length === 3)
    assert(branched(BranchKey(1)).map {
      case (_, (_, partid)) => partid == TaskContext.get.partitionId
    }.collect.exists(_ == false))

    assert(branched(BranchKey(0)).map(_._2._1).collect.toSeq === (1 until 100 by 2))
    assert(branched(BranchKey(0)).partitioner.isEmpty)
    assert(branched(BranchKey(0)).partitions.length === rdd.partitions.length)
    assert(branched(BranchKey(0)).map {
      case (_, (_, partid)) => partid == TaskContext.get.partitionId
    }.collect.forall(_ == true))
  }

  it should "branch by odd or even preserving partitioning" in {
    val rdd = sc.parallelize(0 until 100).map(i => (i, i)).partitionBy(new HashPartitioner(4))
    val branched = rdd.branch(Set(BranchKey(0), BranchKey(1)), iter => {
      iter.map(ii => (Branch(BranchKey(if (ii._2 % 2 == 0) 1 else 0), ii._1), (ii._2, TaskContext.get.partitionId)))
    },
      partitioners = Map(BranchKey(1) -> new HashPartitioner(3)),
      preservesPartitioning = true)

    assert(branched(BranchKey(1)).map(_._2._1).collect.toSeq ===
      rdd.filter(_._2 % 2 == 0).partitionBy(new HashPartitioner(3)).map(_._2).collect.toSeq)
    assert(branched(BranchKey(1)).partitioner.isDefined)
    assert(branched(BranchKey(1)).partitions.length === 3)
    assert(branched(BranchKey(1)).map {
      case (_, (_, partid)) => partid == TaskContext.get.partitionId
    }.collect.exists(_ == false))

    assert(branched(BranchKey(0)).map(_._2._1).collect.toSeq ===
      rdd.filter(_._2 % 2 != 0).map(_._2).collect.toSeq)
    assert(branched(BranchKey(0)).partitioner.isDefined)
    assert(branched(BranchKey(0)).partitions.length === 4)
    assert(branched(BranchKey(0)).map {
      case (_, (_, partid)) => partid == TaskContext.get.partitionId
    }.collect.forall(_ == true))
  }

  it should "branch by odd or even ordering desc" in {
    val rdd = sc.parallelize(0 until 100)
    val branched = rdd.branch(Set(BranchKey(0), BranchKey(1)), iter => {
      iter.map(i => (Branch(BranchKey(if (i % 2 == 0) 1 else 0), i), (i, TaskContext.get.partitionId)))
    },
      keyOrderings = Map(BranchKey(1) -> implicitly[Ordering[Int]].reverse))

    assert(branched(BranchKey(1)).map(_._2._1).collect.toSeq ===
      rdd.mapPartitions(iter => iter.toSeq.filter(_ % 2 == 0).reverse.iterator).collect.toSeq)
    assert(branched(BranchKey(1)).partitioner.isEmpty)
    assert(branched(BranchKey(1)).partitions.length === rdd.partitions.length)
    assert(branched(BranchKey(1)).map {
      case (_, (_, partid)) => partid == TaskContext.get.partitionId
    }.collect.forall(_ == true))

    assert(branched(BranchKey(0)).map(_._2._1).collect.toSeq === (1 until 100 by 2))
    assert(branched(BranchKey(0)).partitioner.isEmpty)
    assert(branched(BranchKey(0)).partitions.length === rdd.partitions.length)
    assert(branched(BranchKey(0)).map {
      case (_, (_, partid)) => partid == TaskContext.get.partitionId
    }.collect.forall(_ == true))
  }
}
