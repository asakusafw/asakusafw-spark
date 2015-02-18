package com.asakusafw.spark.runtime

import org.scalatest.BeforeAndAfterEach
import org.scalatest.Suite

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

trait SparkSugar extends BeforeAndAfterEach { self: Suite =>

  var sc: SparkContext = _

  override def beforeEach() {
    try {
      val conf = new SparkConf
      conf.setMaster("local[*]")
      conf.setAppName(getClass.getName)
      conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      sc = new SparkContext(conf)
    } finally {
      super.beforeEach()
    }
  }

  override def afterEach() {
    try {
      super.afterEach()
    } finally {
      sc.stop
      sc = null
    }
  }
}
