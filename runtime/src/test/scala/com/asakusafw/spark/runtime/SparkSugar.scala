package com.asakusafw.spark.runtime

import org.scalatest.BeforeAndAfterEach
import org.scalatest.Suite

import scala.collection.JavaConversions._

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import com.asakusafw.bridge.stage.StageInfo

trait SparkSugar extends BeforeAndAfterEach { self: Suite =>

  var sc: SparkContext = _

  override def beforeEach() {
    try {
      val conf = new SparkConf
      conf.setMaster("local[*]")
      conf.setAppName(getClass.getName)
      conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      conf.set("spark.kryo.registrator", kryoRegistrator)

      val stageInfo = new StageInfo(
        sys.props("user.name"), "batchId", "flowId", null, "executionId", Map.empty[String, String])
      conf.setHadoopConf(Props.StageInfo, stageInfo.serialize)

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
