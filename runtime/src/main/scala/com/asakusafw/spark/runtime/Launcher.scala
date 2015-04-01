package com.asakusafw.spark.runtime

import org.apache.spark.SparkConf
import org.slf4j.LoggerFactory

import com.asakusafw.bridge.stage.StageInfo

object Launcher {

  val Logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    try {
      assert(args.length >= 5)
      val Array(client, batchId, flowId, executionId, batchArgs) = args.take(5)

      if (Logger.isInfoEnabled) {
        Logger.info(s"SparkClient: ${client}")
        Logger.info(s"batchId: ${batchId}")
        Logger.info(s"flowId: ${flowId}")
        Logger.info(s"executionId: ${executionId}")
        Logger.info(s"batchArgs: ${batchArgs}")
      }

      val sparkClient = Class.forName(client).asSubclass(classOf[SparkClient]).newInstance()

      val sparkConf = new SparkConf

      val stageInfo = new StageInfo(
        sys.props("user.name"), batchId, flowId, null, executionId, batchArgs)
      sparkConf.setHadoopConf(Props.StageInfo, stageInfo.serialize)

      sparkClient.execute(sparkConf)
    } catch {
      case t: Throwable =>
        Logger.error(s"SparkClient throws: ${t.getMessage}", t)
        throw t
    }
  }
}
