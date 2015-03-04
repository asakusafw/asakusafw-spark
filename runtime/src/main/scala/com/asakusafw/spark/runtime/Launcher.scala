package com.asakusafw.spark.runtime

import org.apache.spark.SparkConf

object Launcher {

  def main(args: Array[String]): Unit = {
    assert(args.length > 5)
    val Array(className, batchId, flowId, executionId, batchArgs) = args.take(5)
    val sparkClient = Class.forName(className).asSubclass(classOf[AsakusaSparkClient]).newInstance()

    val sparkConf = new SparkConf
    sparkClient.execute(sparkConf)
  }
}
