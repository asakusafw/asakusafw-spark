package com.asakusafw.spark.runtime

import org.apache.spark.SparkConf
import org.slf4j.LoggerFactory

import com.asakusafw.runtime.stage.StageConstants

object Launcher {

  val Logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
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
    sparkConf.setHadoopConf(StageConstants.PROP_BATCH_ID, batchId)
    sparkConf.setHadoopConf(StageConstants.PROP_FLOW_ID, flowId)
    sparkConf.setHadoopConf(StageConstants.PROP_EXECUTION_ID, executionId)
    sparkConf.setHadoopConf(StageConstants.PROP_ASAKUSA_BATCH_ARGS, batchArgs)
    sparkClient.execute(sparkConf)
  }
}
