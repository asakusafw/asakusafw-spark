package com.asakusafw.spark.runtime

import org.apache.spark.SparkConf

trait AsakusaSparkClient {

  def execute(conf: SparkConf): Int
}
