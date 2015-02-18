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
      case Seq(iter1, iter2, iter3) =>
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
