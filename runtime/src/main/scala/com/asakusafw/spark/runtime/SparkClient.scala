package com.asakusafw.spark.runtime

import org.apache.spark._

abstract class SparkClient {

  def execute(conf: SparkConf): Int = {
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrator", kryoRegistrator)

    val sc = new SparkContext(conf)
    try {
      execute(sc)
    } finally {
      sc.stop()
    }
  }

  def execute(sc: SparkContext): Int

  def kryoRegistrator: String
}
