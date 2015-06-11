package com.asakusafw.spark.runtime

import scala.collection.JavaConversions._

import org.apache.spark.SparkConf
import org.slf4j.LoggerFactory

import com.asakusafw.bridge.launch.LaunchConfiguration
import com.asakusafw.bridge.stage.StageInfo
import com.asakusafw.runtime.core.context.RuntimeContext

object Launcher {

  val Logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    try {
      val cl = Thread.currentThread.getContextClassLoader

      RuntimeContext.set(RuntimeContext.DEFAULT.apply(System.getenv))
      RuntimeContext.get.verifyApplication(cl)
      val conf = LaunchConfiguration.parse(cl, args: _*)

      val sparkClient = conf.getStageClient.asSubclass(classOf[SparkClient]).newInstance()

      val sparkConf = new SparkConf()

      sparkConf.setHadoopConf(StageInfo.KEY_NAME, conf.getStageInfo.serialize)

      conf.getHadoopProperties.foreach {
        case (key, value) => sparkConf.setHadoopConf(key, value)
      }

      conf.getEngineProperties.foreach {
        case (key, value) => sparkConf.set(key, value)
      }

      if (!sparkConf.contains(Props.Parallelism) && !sparkConf.contains("spark.default.parallelism")) {
        if (Logger.isWarnEnabled) {
          Logger.warn(s"`${Props.Parallelism}` is not set, we set parallelism to ${Props.ParallelismFallback}.")
        }
      }

      if (!RuntimeContext.get.isSimulation) {
        sparkClient.execute(sparkConf)
      }
    } catch {
      case t: Throwable =>
        Logger.error(s"SparkClient throws: ${t.getMessage}", t)
        throw t
    }
  }
}
