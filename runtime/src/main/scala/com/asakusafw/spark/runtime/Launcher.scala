/*
 * Copyright 2011-2017 Asakusa Framework Team.
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
package com.asakusafw.spark.runtime

import scala.collection.JavaConversions._

import org.apache.spark.SparkConf
import org.slf4j.LoggerFactory

import com.asakusafw.iterative.launch.IterativeLaunchConfiguration
import com.asakusafw.runtime.core.context.RuntimeContext

object Launcher {

  val Logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    try {
      val cl = Thread.currentThread.getContextClassLoader

      RuntimeContext.set(RuntimeContext.DEFAULT.apply(System.getenv))
      RuntimeContext.get.verifyApplication(cl)
      val conf = IterativeLaunchConfiguration.parse(cl, args: _*)

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
        val status = sparkClient.execute(sparkConf, conf.getStageInfo)
        if (status != 0) {
          throw new RuntimeException(s"Spark execution returned non-zero value: ${status}")
        }
      }
    } catch {
      case t: Throwable =>
        Logger.error(s"SparkClient throws: ${t.getMessage}", t)
        throw t
    }
  }
}
