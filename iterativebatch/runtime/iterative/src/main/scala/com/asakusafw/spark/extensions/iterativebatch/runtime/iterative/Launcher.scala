/*
 * Copyright 2011-2015 Asakusa Framework Team.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.asakusafw.spark.extensions.iterativebatch.runtime
package iterative

import scala.collection.JavaConversions._
import scala.util.control.NonFatal

import org.apache.spark.SparkConf
import org.slf4j.LoggerFactory

import com.asakusafw.bridge.launch.LaunchConfiguration
import com.asakusafw.bridge.stage.StageInfo
import com.asakusafw.runtime.core.context.RuntimeContext
import com.asakusafw.spark.runtime.`package`._

import com.asakusafw.spark.extensions.iterativebatch.runtime.iterative.SparkClient.RoundConf

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

      conf.getHadoopProperties.foreach {
        case (key, value) => sparkConf.setHadoopConf(key, value)
      }

      conf.getEngineProperties.foreach {
        case (key, value) => sparkConf.set(key, value)
      }

      if (!sparkConf.contains(Props.Parallelism)
        && !sparkConf.contains("spark.default.parallelism")) {
        if (Logger.isWarnEnabled) {
          Logger.warn(
            s"`${Props.Parallelism}` is not set, " +
              s"we set parallelism to ${Props.ParallelismFallback}.")
        }
      }

      if (!RuntimeContext.get.isSimulation) {
        val stageInfos = Seq(RoundConf(conf.getStageInfo, Map.empty)) // TODO get StageInfos
        sparkClient.execute(sparkConf, stageInfos)
      }
    } catch {
      case NonFatal(t) =>
        Logger.error(s"SparkClient throws: ${t.getMessage}", t)
        throw t
    }
  }
}
