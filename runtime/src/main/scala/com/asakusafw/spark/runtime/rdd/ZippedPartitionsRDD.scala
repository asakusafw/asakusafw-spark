package com.asakusafw.spark.runtime.rdd

import scala.reflect.ClassTag

import org.apache.spark._
import org.apache.spark.rdd.RDD

import org.apache.spark.rdd.backdoor._

class ZippedPartitionsRDD[V: ClassTag](
  sc: SparkContext,
  var f: (Seq[Iterator[_]]) => Iterator[V],
  var _rdds: Seq[RDD[_]],
  preservesPartitioning: Boolean = false)
    extends ZippedPartitionsBaseRDD[V](sc, _rdds, preservesPartitioning) {

  override def compute(s: Partition, context: TaskContext): Iterator[V] = {
    val partitions = s.asInstanceOf[ZippedPartitionsPartition].partitions
    f(rdds.zipWithIndex.map {
      case (rdd, i) => rdd.iterator(partitions(i), context)
    })
  }

  override def clearDependencies() {
    super.clearDependencies()
    _rdds = null
    f = null
  }
}
