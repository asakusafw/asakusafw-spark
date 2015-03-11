package com.asakusafw.spark.runtime

import org.scalatest.BeforeAndAfterEach
import org.scalatest.Suite

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import com.asakusafw.runtime.stage.StageConstants

trait SparkSugar extends BeforeAndAfterEach { self: Suite =>

  var sc: SparkContext = _

  override def beforeEach() {
    try {
      val conf = new SparkConf
      conf.setMaster("local[*]")
      conf.setAppName(getClass.getName)
      conf.setHadoopConf(StageConstants.PROP_BATCH_ID, "batchId")
      conf.setHadoopConf(StageConstants.PROP_FLOW_ID, "flowId")
      conf.setHadoopConf(StageConstants.PROP_EXECUTION_ID, "executionId")
      conf.setHadoopConf(StageConstants.PROP_ASAKUSA_BATCH_ARGS, "batchArgs")
      conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      conf.set("spark.kryo.registrator", kryoRegistrator)
      sc = new SparkContext(conf)
    } finally {
      super.beforeEach()
    }
  }

  def kryoRegistrator: String = "com.asakusafw.spark.runtime.serializer.KryoRegistrator"

  override def afterEach() {
    try {
      super.afterEach()
    } finally {
      sc.stop
      sc = null
    }
  }
}
