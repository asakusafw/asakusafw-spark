package com.asakusafw.spark.runtime

import org.apache.spark.SparkConf
import org.slf4j.LoggerFactory

import com.asakusafw.bridge.stage.StageInfo

object Launcher {

  val Logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    try {
      require(args.length >= 5,
        s"The size of arguments should be more than 5: ${args.length}")
      val Array(client, batchId, flowId, executionId, batchArgs) = args.take(5)

      if (Logger.isInfoEnabled) {
        Logger.info(s"SparkClient: ${client}")
        Logger.info(s"batchId: ${batchId}")
        Logger.info(s"flowId: ${flowId}")
        Logger.info(s"executionId: ${executionId}")
        Logger.info(s"batchArgs: ${batchArgs}")
      }

      val sparkClient = Class.forName(client, false, Thread.currentThread.getContextClassLoader)
        .asSubclass(classOf[SparkClient])
        .newInstance()

      val sparkConf = new SparkConf()

      val stageInfo = new StageInfo(
        sys.props("user.name"), batchId, flowId, null, executionId, batchArgs)
      sparkConf.setHadoopConf(StageInfo.KEY_NAME, stageInfo.serialize)

      if (sys.props.contains(Props.Parallelism)) {
        sparkConf.set(Props.Parallelism, sys.props(Props.Parallelism))
      } else if (!sparkConf.contains("spark.default.parallelism")) {
        if (Logger.isWarnEnabled) {
          Logger.warn(s"`${Props.Parallelism}` is not set, we set parallelism to 2.")
        }
      }

      for {
        prop <- Seq(
          Props.ParallelismScaleSmall,
          Props.ParallelismScaleLarge,
          Props.ParallelismScaleHuge)
        value <- sys.props.get(prop)
      } {
        sparkConf.set(prop, value)
      }

      sparkClient.execute(sparkConf)
    } catch {
      case t: Throwable =>
        Logger.error(s"SparkClient throws: ${t.getMessage}", t)
        throw t
    }
  }
}
