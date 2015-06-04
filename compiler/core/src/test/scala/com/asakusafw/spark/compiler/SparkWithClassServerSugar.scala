package com.asakusafw.spark.compiler

import org.scalatest.BeforeAndAfterEach
import org.scalatest.Suite

import scala.collection.JavaConversions._

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast

import com.asakusafw.bridge.stage.StageInfo
import com.asakusafw.runtime.stage.StageConstants
import com.asakusafw.spark.runtime._

trait SparkWithClassServerSugar extends BeforeAndAfterEach { self: Suite =>

  var cl: ClassLoader = _

  var classServer: ClassServer = _

  var sc: SparkContext = _
  var hadoopConf: Broadcast[Configuration] = _

  override def beforeEach() {
    try {
      super.beforeEach()
    } finally {
      cl = Thread.currentThread().getContextClassLoader()

      val conf = new SparkConf
      conf.setMaster("local[8]")
      conf.setAppName(getClass.getName)
      conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      conf.set("spark.kryo.registrator", kryoRegistrator)

      val stageInfo = new StageInfo(
        sys.props("user.name"), "batchId", "flowId", null, "executionId", Map.empty[String, String])
      conf.setHadoopConf(Props.StageInfo, stageInfo.serialize)

      conf.set(Props.Parallelism, 8.toString)

      classServer = new ClassServer(cl, conf)
      val uri = classServer.start()
      Thread.currentThread().setContextClassLoader(classServer.classLoader)

      conf.set("spark.repl.class.uri", uri)
      sc = new SparkContext(conf)
      hadoopConf = sc.broadcast(sc.hadoopConfiguration)
    }
  }

  def kryoRegistrator: String = "com.asakusafw.spark.runtime.serializer.KryoRegistrator"

  override def afterEach() {
    try {
      Thread.currentThread().setContextClassLoader(cl)
      hadoopConf = null
      sc.stop
      sc = null
      classServer.stop
      classServer = null
    } finally {
      super.afterEach
    }
  }
}
