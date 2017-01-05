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
package com.asakusafw.spark.extensions.iterativebatch.runtime
package graph

import scala.concurrent.{ Await, ExecutionContext, Future }
import scala.concurrent.duration.Duration

import org.apache.hadoop.conf.Configuration
import org.slf4j.LoggerFactory

import com.asakusafw.bridge.stage.StageInfo
import com.asakusafw.runtime.directio.hadoop.HadoopDataSourceUtil
import com.asakusafw.spark.runtime.{ JobContext, RoundContext }
import com.asakusafw.spark.runtime.graph._

abstract class DirectOutputCommitForIterative(
  prepares: Set[IterativeAction[Unit]])(
    implicit val jobContext: JobContext) extends IterativeAction[Unit] {
  self: CacheStrategy[Seq[RoundContext], Future[Unit]] =>

  private val Logger = LoggerFactory.getLogger(getClass)

  override val label = getClass.getSimpleName

  def basePaths: Set[String]

  override protected def doPerform(
    origin: RoundContext,
    rcs: Seq[RoundContext])(implicit ec: ExecutionContext): Future[Unit] = {

    Future.sequence(prepares.map(_.perform(origin, rcs))).map { _ =>
      val conf = origin.hadoopConf.value
      val stageInfo = StageInfo.deserialize(conf.get(StageInfo.KEY_NAME))
      val repository = HadoopDataSourceUtil.loadRepository(conf)

      val containerPaths = rcs.flatMap { rc =>
        val conf = rc.hadoopConf.value
        val stageInfo = StageInfo.deserialize(conf.get(StageInfo.KEY_NAME))
        basePaths.map { basePath =>
          repository.getContainerPath(stageInfo.resolveUserVariables(basePath))
        }
      }.toSet

      val transactionManager = createTransactionManager(stageInfo, conf)
      try {
        val commits = containerPaths.map { containerPath =>
          val id = repository.getRelatedId(containerPath)
          val source = repository.getRelatedDataSource(containerPath)
          val context = transactionManager.acquire(id)

          { () =>
            if (Logger.isDebugEnabled) {
              Logger.debug(s"commiting Direct I/O file output: ${containerPath}/*")
            }
            source.commitTransactionOutput(context)
            source.cleanupTransactionOutput(context)
            transactionManager.release(context)
          }
        }

        transactionManager.begin()

        Await.result(Future.sequence(commits.map(commit => Future(commit()))), Duration.Inf)
      } finally {
        transactionManager.end()
      }
    }
  }

  private def createTransactionManager(
    stageInfo: StageInfo, conf: Configuration): TransactionManager = {
    new TransactionManager(
      conf,
      stageInfo.getExecutionId,
      Map(
        "User Name" -> stageInfo.getUserName,
        "Batch ID" -> stageInfo.getBatchId,
        "Flow ID" -> stageInfo.getFlowId,
        "Execution ID" -> stageInfo.getExecutionId,
        "Stage ID" -> stageInfo.getStageId,
        "Batch Arguments" -> stageInfo.getBatchArguments().toString()))
  }
}
