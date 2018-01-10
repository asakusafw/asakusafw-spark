/*
 * Copyright 2011-2018 Asakusa Framework Team.
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
package listener

import scala.util.Try

import org.slf4j.LoggerFactory

import com.asakusafw.spark.runtime.JobContext.{ InputCounter, OutputCounter }

class OutputCounters extends SparkClient.Listener {

  private val Logger = LoggerFactory.getLogger(getClass)

  override def onJobCompleted(jobContext: JobContext, result: Try[Int]): Unit = {
    if (result.isSuccess && Logger.isInfoEnabled) {

      jobContext.inputStatistics.get(InputCounter.Direct).foreach { statistics =>
        Logger.info(s"Direct I/O file input: ${statistics.size} entries")
        statistics.toSeq.sortBy(_._1).foreach {
          case (name, statistics) =>
            Logger.info(s"  ${name}:")
            Logger.info(f"    number of input records: ${statistics.records}%,d")
        }
        Logger.info(s"  (TOTAL):")
        Logger.info(f"    number of input records: ${statistics.map(_._2.records).sum}%,d")
      }

      jobContext.outputStatistics.get(OutputCounter.Direct).foreach { statistics =>
        Logger.info(s"Direct I/O file output: ${statistics.size} entries")
        statistics.toSeq.sortBy(_._1).foreach {
          case (name, statistics) =>
            Logger.info(s"  ${name}:")
            Logger.info(f"    number of output files: ${statistics.files}%,d")
            Logger.info(f"    output file size in bytes: ${statistics.bytes}%,d")
            Logger.info(f"    number of output records: ${statistics.records}%,d")
        }
        Logger.info(s"  (TOTAL):")
        Logger.info(f"    number of output files: ${statistics.map(_._2.files).sum}%,d")
        Logger.info(f"    output file size in bytes: ${statistics.map(_._2.bytes).sum}%,d")
        Logger.info(f"    number of output records: ${statistics.map(_._2.records).sum}%,d")
      }

      jobContext.inputStatistics.get(InputCounter.External).foreach { statistics =>
        Logger.info(s"External I/O file input: ${statistics.size} entries")
        statistics.toSeq.sortBy(_._1).foreach {
          case (name, statistics) =>
            Logger.info(s"  ${name}:")
            Logger.info(f"    number of input records: ${statistics.records}%,d")
        }
        Logger.info(s"  (TOTAL):")
        Logger.info(f"    number of input records: ${statistics.map(_._2.records).sum}%,d")
      }

      jobContext.outputStatistics.get(OutputCounter.External).foreach { statistics =>
        Logger.info(s"External I/O file output: ${statistics.size} entries")
        statistics.toSeq.sortBy(_._1).foreach {
          case (name, statistics) =>
            Logger.info(s"  ${name}:")
            Logger.info(f"    number of output records: ${statistics.records}%,d")
        }
        Logger.info(s"  (TOTAL):")
        Logger.info(f"    number of output records: ${statistics.map(_._2.records).sum}%,d")
      }
    }
  }
}
