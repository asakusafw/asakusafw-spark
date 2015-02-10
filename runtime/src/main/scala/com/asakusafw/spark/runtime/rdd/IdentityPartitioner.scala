package com.asakusafw.spark.runtime.rdd

import org.apache.spark.Partitioner
import org.apache.spark.TaskContext

case class IdentityPartitioner(numPartitions: Int) extends Partitioner {

  override def getPartition(key: Any): Int = {
    TaskContext.get.partitionId
  }
}
