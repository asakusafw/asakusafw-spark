/*
 * Copyright 2011-2016 Asakusa Framework Team.
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

import java.util.Arrays

import scala.annotation.meta.param
import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration.Duration

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Writable
import org.apache.spark.{ Partitioner, SparkContext, TaskContext }
import org.slf4j.LoggerFactory

import org.apache.spark.backdoor._
import org.apache.spark.executor.backdoor._
import org.apache.spark.util.backdoor._

import com.asakusafw.bridge.stage.StageInfo
import com.asakusafw.runtime.directio.{
  Counter,
  DataDefinition,
  DataFormat,
  DirectDataSource,
  OutputAttemptContext
}
import com.asakusafw.runtime.directio.hadoop.HadoopDataSourceUtil
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.runtime.value.StringOption
import com.asakusafw.spark.runtime.RoundContext
import com.asakusafw.spark.runtime.directio._
import com.asakusafw.spark.runtime.graph._
import com.asakusafw.spark.runtime.io.WritableSerDe
import com.asakusafw.spark.runtime.rdd._

import resource._

abstract class DirectOutputPrepareForIterative[T <: DataModel[T] with Writable: Manifest](
  @(transient @param) setup: IterativeAction[Unit],
  @(transient @param) prepare: DirectOutputPrepareEachForIterative[T])(
    implicit @transient val sc: SparkContext) extends IterativeAction[Unit] {
  self: CacheStrategy[Seq[RoundContext], Future[Unit]] =>

  private val Logger = LoggerFactory.getLogger(getClass)

  def formatType: Class[_ <: DataFormat[T]]

  override protected def doPerform(
    origin: RoundContext,
    rcs: Seq[RoundContext])(implicit ec: ExecutionContext): Future[Unit] = {

    Future.sequence(rcs.map(rc => prepare.perform(rc)).toSet.toSeq)
      .zip(setup.perform(origin, rcs))
      .map(_._1)
      .map { prepares =>

        sc.clearCallSite()
        sc.setCallSite(
          CallSite(origin.roundId.map(r => s"${label}: [${r}]").getOrElse(label), origin.toString))

        prepare.confluent(prepares).foreachPartition { iter =>
          val conf = origin.hadoopConf.value
          val stageInfo = StageInfo.deserialize(conf.get(StageInfo.KEY_NAME))
          val repository = HadoopDataSourceUtil.loadRepository(conf)

          val definition = BasicDataDefinition(new HadoopObjectFactory(conf), formatType)
          val taskContext = TaskContext.get
          val attemptId = s"s${taskContext.stageId}-p${taskContext.partitionId}"

          val bytes = new Counter()
          val records = new Counter()

          val buff = iter.buffered
          val basePathOpt = new StringOption()
          val pathOpt = new StringOption()

          while (buff.hasNext) {
            val currentBasePath = buff.head._1.grouping
            WritableSerDe.deserialize(currentBasePath, basePathOpt)

            val basePathBytes =
              StringOption.getBytesLength(currentBasePath, 0, currentBasePath.length)

            def sameBasePath: Boolean = {
              StringOption.compareBytes(
                currentBasePath, 0, basePathBytes,
                buff.head._1.grouping, 0, basePathBytes) == 0
            }

            val basePath = basePathOpt.getAsString()

            val sourceId = repository.getRelatedId(basePath)
            val containerPath = repository.getContainerPath(basePath)
            val componentPath = repository.getComponentPath(basePath)
            val dataSource = repository.getRelatedDataSource(containerPath)

            val context = new OutputAttemptContext(
              stageInfo.getExecutionId, attemptId, sourceId, new Counter())

            dataSource.setupAttemptOutput(context)

            try {
              while (buff.hasNext && sameBasePath) {
                val currentPath = buff.head._1.grouping
                WritableSerDe.deserialize(currentPath, basePathBytes, pathOpt)

                def sameResourcePath: Boolean = {
                  StringOption.compareBytes(
                    currentPath, basePathBytes, currentPath.length,
                    buff.head._1.grouping, basePathBytes, buff.head._1.grouping.length) == 0
                }

                for {
                  output <- managed(
                    dataSource.openOutput(
                      context, definition, componentPath, pathOpt.getAsString, bytes))
                } {
                  var rs = 0L
                  while (buff.hasNext && sameBasePath && sameResourcePath) {
                    rs += 1L
                    output.write(buff.next()._2)
                  }
                  records.add(rs)
                }
              }

              dataSource.commitAttemptOutput(context)
            } catch {
              case t: Throwable =>
                Logger.error(
                  "error occurred while processing Direct file output: " +
                    s"basePath=${basePath}, context=${context}",
                  t)
            } finally {
              dataSource.cleanupAttemptOutput(context)
            }
          }

          taskContext.taskMetrics.outputMetrics.setBytesWritten(
            taskContext.taskMetrics.outputMetrics.bytesWritten + bytes.get)
          taskContext.taskMetrics.outputMetrics.setRecordsWritten(
            taskContext.taskMetrics.outputMetrics.recordsWritten + records.get)
        }
      }
  }
}
