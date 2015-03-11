package com.asakusafw.spark.runtime
package rdd

import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._

@RunWith(classOf[JUnitRunner])
class SortMergeCoGroupSpecTest extends SortMergeCoGroupSpec

class SortMergeCoGroupSpec extends FlatSpec with SparkSugar {

  import SortMergeCoGroupSpec._

  behavior of "SortMergeCoGroup"

  it should "cogroup rdds" in {
    val rdd1 = sc.parallelize(0 until 100).map(i =>
      ((i.toString, 10), i.toString))
    val rdd2 = sc.parallelize(0 until 100).flatMap(i => Seq(
      ((i.toString, 10), i * 10),
      ((i.toString, 0), i)))

    val part = new GroupingPartitioner(2)
    val ord3 = Ordering.Tuple2(implicitly[Ordering[String]], implicitly[Ordering[Int]].reverse)
    val rdd3: RDD[((String, Int), Int)] =
      new ShuffledRDD(
        sc.parallelize(0 until 100).flatMap(i => Seq(
          ((i.toString, 0), i.toLong),
          ((i.toString, 10), i.toLong * 10))),
        part)
        .setKeyOrdering(ord3)

    val grouping = new GroupingOrdering
    val cogrouped = smcogroup(
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
    cogrouped.collect.zip((part0 ++ part1).map(k => (k, 0))).foreach {
      case ((actualKey, actualValues), key @ (k, _)) =>
        assert(grouping.compare(actualKey, key) === 0)
        assert(actualValues.size === 3)
        assert(actualValues(0) === Seq(k))
        assert(actualValues(1) === Seq(k.toInt, k.toInt * 10))
        assert(actualValues(2) === Seq(k.toLong * 10, k.toLong))
    }
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