/*
 * Copyright 2011-2019 Asakusa Framework Team.
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
package iterative.listener

import scala.util.{ Failure, Success, Try }

import org.slf4j.LoggerFactory

import com.asakusafw.spark.runtime.RoundContext

class Logger extends IterativeBatchExecutor.Listener {

  private val Logger = LoggerFactory.getLogger(getClass)

  override def onExecutorStart(): Unit = {
    if (Logger.isInfoEnabled) {
      Logger.info("IterativaBatchExecutor started.")
    }
  }

  override def onRoundSubmitted(rc: RoundContext): Unit = {
    if (Logger.isInfoEnabled) {
      Logger.info(s"Round[${rc}] is submitted.")
    }
  }

  override def onRoundStart(rc: RoundContext): Unit = {
    if (Logger.isInfoEnabled) {
      Logger.info(s"Round[${rc}] started.")
    }
  }

  override def onRoundCompleted(rc: RoundContext, result: Try[Unit]): Unit = {
    result match {
      case Success(_) =>
        if (Logger.isInfoEnabled) {
          Logger.info(s"Round[${rc}] successfully completed.")
        }
      case Failure(t) =>
        if (Logger.isErrorEnabled) {
          Logger.error(s"Round[${rc}] failed.", t)
        }
    }
  }

  override def onExecutorStop(): Unit = {
    if (Logger.isInfoEnabled) {
      Logger.info("IterativaBatchExecutor stopped.")
    }
  }
}
