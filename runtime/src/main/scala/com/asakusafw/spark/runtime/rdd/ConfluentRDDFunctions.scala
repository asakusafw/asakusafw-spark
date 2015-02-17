package com.asakusafw.spark.runtime.rdd

import scala.reflect.ClassTag

import org.apache.spark.Partitioner
import org.apache.spark.rdd._

class ConfluentRDDFunctions[K: ClassTag, V: ClassTag](self: RDD[(K, V)]) {

  def confluent(rdds: Seq[RDD[(K, V)]], part: Partitioner, ord: Ordering[K]): RDD[(K, V)] = {

    def shuffle(rdd: RDD[(K, V)]): RDD[(K, V)] = {
      if (rdd.partitioner == Some(part)) {
        rdd
      } else {
        new ShuffledRDD(rdd, part).setKeyOrdering(ord)
      }
    }

    (shuffle(self) /: rdds.map(shuffle)) {
      case (left, right) =>
        left.zipPartitions(right) {
          case (leftIter, rightIter) =>
            val bufLeftIter = leftIter.buffered
            val bufRightIter = rightIter.buffered
            Iterator.continually {
              if (bufLeftIter.hasNext && bufRightIter.hasNext) {
                Some(if (ord.lt(bufLeftIter.head._1, bufRightIter.head._1)) {
                  bufLeftIter.next
                } else {
                  bufRightIter.next
                })
              } else if (bufLeftIter.hasNext) {
                Some(bufLeftIter.next)
              } else if (bufRightIter.hasNext) {
                Some(bufRightIter.next)
              } else {
                None
              }
            }.takeWhile(_.isDefined).map(_.get)
        }
    }
  }
}
