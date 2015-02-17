package com.asakusafw.spark.runtime

import scala.language.implicitConversions
import scala.reflect.ClassTag
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.ShuffledRDD

package object rdd {

  implicit def rddToBranchRDDFunctions[T: ClassTag](rdd: RDD[T]): BranchRDDFunctions[T] = {
    new BranchRDDFunctions(rdd)
  }

  implicit def rddToConfluentRDDFunctions[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)]): ConfluentRDDFunctions[K, V] = {
    new ConfluentRDDFunctions(rdd)
  }

  def confluent[K: ClassTag, V: ClassTag](
    rdds: Seq[RDD[(K, V)]], part: Partitioner, ord: Ordering[K]): RDD[(K, V)] = {
    assert(rdds.size > 0)
    rdds.head.confluent(rdds.tail, part, ord)
  }
}
