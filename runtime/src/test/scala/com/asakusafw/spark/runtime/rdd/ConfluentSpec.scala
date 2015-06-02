package com.asakusafw.spark.runtime
package rdd

import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner

import org.apache.spark._
import org.apache.spark.rdd._

import com.asakusafw.runtime.value.IntOption

@RunWith(classOf[JUnitRunner])
class ConfluentSpecTest extends ConfluentSpec

class ConfluentSpec extends FlatSpec with SparkSugar {

  import ConfluentSpec._

  behavior of "Confluent"

  it should "confluent rdds" in {
    val rdd1 = sc.parallelize(0 until 100).map(i => ((i.toString, 0), i))
    val rdd2 = sc.parallelize(0 until 100).flatMap(i => Seq(((i.toString, 1), i + 100), ((i.toString, 2), i + 200)))

    val part = new GroupingPartitioner(2)
    val ord = implicitly[Ordering[(String, Int)]]
    val rdd3: RDD[((String, Int), Int)] =
      new ShuffledRDD(
        sc.parallelize(0 until 100).flatMap(i => Seq(((i.toString, 4), i + 400), ((i.toString, 3), i + 300))), part)
        .setKeyOrdering(ord)

    val confluented = confluent(Seq(rdd1, rdd2, rdd3), part, Some(ord))
    val (part0, part1) = (0 until 100).sortBy(_.toString).partition { i =>
      val part = i.toString.hashCode % 2
      (if (part < 0) part + 2 else part) == 0
    }
    assert(confluented.collect ===
      (part0 ++ part1).flatMap(i => (0 until 5).map(j => ((i.toString, j), i + 100 * j))))
  }

  it should "confluent rdds of mutable values" in {
    val part = new GroupingPartitioner(2)
    val ord = implicitly[Ordering[(String, Int)]]

    val rdd1: RDD[((String, Int), IntOption)] =
      new ShuffledRDD(
        sc.parallelize(0 until 10).map(i => ((i.toString, 0), i)),
        part)
        .setKeyOrdering(ord)
        .mapPartitions(new Func, preservesPartitioning = true)

    val rdd2: RDD[((String, Int), IntOption)] =
      new ShuffledRDD(
        sc.parallelize(0 until 10).flatMap(i => Seq(((i.toString, 1), i + 10), ((i.toString, 2), i + 20))),
        part)
        .setKeyOrdering(ord)
        .mapPartitions(new Func, preservesPartitioning = true)

    val rdd3: RDD[((String, Int), IntOption)] =
      new ShuffledRDD(
        sc.parallelize(0 until 10).flatMap(i => Seq(((i.toString, 4), i + 40), ((i.toString, 3), i + 30))),
        part)
        .setKeyOrdering(ord)
        .mapPartitions(new Func, preservesPartitioning = true)

    val confluented = confluent(Seq(rdd1, rdd2, rdd3), part, Some(ord))
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

  class Func extends Function1[Iterator[((String, Int), Int)], Iterator[((String, Int), IntOption)]] with Serializable {
    @transient var i: IntOption = _
    def intOption: IntOption = {
      if (i == null) {
        i = new IntOption()
      }
      i
    }
    override def apply(iter: Iterator[((String, Int), Int)]): Iterator[((String, Int), IntOption)] = {
      iter.map { value =>
        intOption.modify(value._2)
        (value._1, intOption)
      }
    }
  }
}
