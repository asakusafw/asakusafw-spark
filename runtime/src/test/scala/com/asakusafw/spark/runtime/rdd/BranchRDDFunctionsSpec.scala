package com.asakusafw.spark.runtime
package rdd

import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner

import org.apache.spark.{ HashPartitioner, TaskContext }
import org.apache.spark.SparkContext._

@RunWith(classOf[JUnitRunner])
class BranchRDDFunctionsSpecTest extends BranchRDDFunctionsSpec

class BranchRDDFunctionsSpec extends FlatSpec with SparkSugar {

  behavior of "BranchRDDFunctions"

  it should "branch by odd or even" in {
    val rdd = sc.parallelize(0 until 100)
    val branched = rdd.branch(Set(true, false), iter => {
      iter.map(i => ((i % 2 == 0, null), (i, TaskContext.get.partitionId)))
    })

    assert(branched(true).map(_._2._1).collect.toSeq === (0 until 100 by 2))
    assert(branched(true).partitioner.isEmpty)
    assert(branched(true).partitions.length === rdd.partitions.length)
    assert(branched(true).map {
      case (_, (_, partid)) => partid == TaskContext.get.partitionId
    }.collect.forall(_ == true))

    assert(branched(false).map(_._2._1).collect.toSeq === (1 until 100 by 2))
    assert(branched(false).partitioner.isEmpty)
    assert(branched(false).partitions.length === rdd.partitions.length)
    assert(branched(false).map {
      case (_, (_, partid)) => partid == TaskContext.get.partitionId
    }.collect.forall(_ == true))
  }

  it should "branch and shuffle by odd or even" in {
    val rdd = sc.parallelize(0 until 100)
    val branched = rdd.branch(Set(true, false), iter => {
      iter.map(i => ((i % 2 == 0, i), (i, TaskContext.get.partitionId)))
    },
      partitioners = Map(true -> new HashPartitioner(3)))

    assert(branched(true).map(_._2._1).collect.toSeq ===
      rdd.filter(_ % 2 == 0).map(i => (i, i)).partitionBy(new HashPartitioner(3)).map(_._2).collect.toSeq)
    assert(branched(true).partitioner.isDefined)
    assert(branched(true).partitions.length === 3)
    assert(branched(true).map {
      case (_, (_, partid)) => partid == TaskContext.get.partitionId
    }.collect.exists(_ == false))

    assert(branched(false).map(_._2._1).collect.toSeq === (1 until 100 by 2))
    assert(branched(false).partitioner.isEmpty)
    assert(branched(false).partitions.length === rdd.partitions.length)
    assert(branched(false).map {
      case (_, (_, partid)) => partid == TaskContext.get.partitionId
    }.collect.forall(_ == true))
  }

  it should "branch by odd or even preserving partitioning" in {
    val rdd = sc.parallelize(0 until 100).map(i => (i, i)).partitionBy(new HashPartitioner(4))
    val branched = rdd.branch(Set(true, false), iter => {
      iter.map(ii => ((ii._2 % 2 == 0, ii._1), (ii._2, TaskContext.get.partitionId)))
    },
      partitioners = Map(true -> new HashPartitioner(3)),
      preservesPartitioning = true)

    assert(branched(true).map(_._2._1).collect.toSeq ===
      rdd.filter(_._2 % 2 == 0).partitionBy(new HashPartitioner(3)).map(_._2).collect.toSeq)
    assert(branched(true).partitioner.isDefined)
    assert(branched(true).partitions.length === 3)
    assert(branched(true).map {
      case (_, (_, partid)) => partid == TaskContext.get.partitionId
    }.collect.exists(_ == false))

    assert(branched(false).map(_._2._1).collect.toSeq ===
      rdd.filter(_._2 % 2 != 0).map(_._2).collect.toSeq)
    assert(branched(false).partitioner.isDefined)
    assert(branched(false).partitions.length === 4)
    assert(branched(false).map {
      case (_, (_, partid)) => partid == TaskContext.get.partitionId
    }.collect.forall(_ == true))
  }
}
