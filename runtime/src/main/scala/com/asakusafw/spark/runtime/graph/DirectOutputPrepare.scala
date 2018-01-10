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
package graph

import java.lang.{ StringBuilder => JStringBuilder }
import java.util.Arrays

import scala.annotation.meta.param
import scala.concurrent.{ ExecutionContext, Future }

import org.apache.hadoop.io.Writable
import org.apache.spark.{ Partitioner, TaskContext }
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

import org.apache.spark.executor.backdoor._

import com.asakusafw.bridge.stage.StageInfo
import com.asakusafw.runtime.directio.{
  Counter,
  DataDefinition,
  DataFormat,
  OutputAttemptContext
}
import com.asakusafw.runtime.directio.hadoop.HadoopDataSourceUtil
import com.asakusafw.runtime.io.ModelOutput
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.runtime.value.{ StringOption, ValueOption }
import com.asakusafw.spark.runtime.JobContext.OutputCounter.Direct
import com.asakusafw.spark.runtime.directio._
import com.asakusafw.spark.runtime.io.WritableSerDe
import com.asakusafw.spark.runtime.rdd.{ BranchKey, ShuffleKey }

import resource._

abstract class DirectOutputPrepare[T: Manifest](
  @(transient @param) setup: Action[Unit],
  @(transient @param) prevs: Seq[(Source, BranchKey)])(
    implicit val jobContext: JobContext) extends Action[Unit] {
  self: CacheStrategy[RoundContext, Future[Unit]] =>

  protected val Logger = LoggerFactory.getLogger(getClass)

  def name: String

  def basePath: String

  def formatType: Class[_ <: DataFormat[T]]

  private val statistics = jobContext.getOrNewOutputStatistics(Direct, name)

  override protected def doPerform(
    rc: RoundContext)(implicit ec: ExecutionContext): Future[Unit] = {

    val rdds = prevs.map {
      case (source, branchKey) =>
        source.compute(rc).apply(branchKey).map(_().asInstanceOf[RDD[(_, _)]])
    }

    (if (rdds.size == 1) {
      rdds.head
    } else {
      Future.sequence(rdds).map { prevs =>
        val zipped =
          prevs.groupBy(_.partitions.length).map {
            case (_, rdds) =>
              rdds.reduce(_.zipPartitions(_, preservesPartitioning = false)(_ ++ _))
          }.toSeq
        if (zipped.size == 1) {
          zipped.head
        } else {
          jobContext.sparkContext.union(zipped)
        }
      }
    })
      .zip(setup.perform(rc))
      .map(_._1)
      .map(_.map(_._2.asInstanceOf[T]))
      .map(doPrepare(rc, _))
  }

  protected def doPrepare(rc: RoundContext, prev: RDD[T]): Unit

  protected def doPreparePartition(
    rc: RoundContext)(
      block: (StageInfo, OutputAttemptContext, String => (ModelOutput[T] => Long) => Unit) => Unit): Unit = { // scalastyle:ignore
    val conf = rc.hadoopConf.value
    val stageInfo = StageInfo.deserialize(conf.get(StageInfo.KEY_NAME))
    val repository = HadoopDataSourceUtil.loadRepository(conf)

    val basePath = stageInfo.resolveUserVariables(this.basePath)

    val sourceId = repository.getRelatedId(basePath)
    val containerPath = repository.getContainerPath(basePath)
    val componentPath = repository.getComponentPath(basePath)
    val dataSource = repository.getRelatedDataSource(containerPath)
    val definition = BasicDataDefinition(new HadoopObjectFactory(conf), formatType)
    val taskContext = TaskContext.get
    val attemptId = s"s${taskContext.stageId}-p${taskContext.partitionId}"
    val context = new OutputAttemptContext(
      stageInfo.getExecutionId, attemptId, sourceId, new Counter())

    dataSource.setupAttemptOutput(context)

    try {
      val bytes = new Counter()
      val records = new Counter()
      block(stageInfo, context, { resourcePath =>
        withOutput =>
          for {
            output <- managed(
              dataSource.openOutput(
                context, definition, componentPath, resourcePath, bytes))
          } {
            records.add(withOutput(output))
          }

          statistics.addFile(s"${basePath}/${resourcePath}")
      })
      dataSource.commitAttemptOutput(context)

      taskContext.taskMetrics.outputMetrics.setBytesWritten(
        taskContext.taskMetrics.outputMetrics.bytesWritten + bytes.get)
      taskContext.taskMetrics.outputMetrics.setRecordsWritten(
        taskContext.taskMetrics.outputMetrics.recordsWritten + records.get)

      statistics.addBytes(bytes.get)
      statistics.addRecords(records.get)
    } catch {
      case t: Throwable =>
        Logger.error(
          "error occurred while processing Direct file output: " +
            s"basePath=${basePath}, context=${context}",
          t)
        throw t
    } finally {
      dataSource.cleanupAttemptOutput(context)
    }
  }
}

abstract class DirectOutputPrepareFlat[T: Manifest](
  setup: Action[Unit],
  prevs: Seq[(Source, BranchKey)])(
    implicit jobContext: JobContext)
  extends DirectOutputPrepare[T](setup, prevs) {
  self: CacheStrategy[RoundContext, Future[Unit]] =>

  private val Placeholder: String = "*"

  def resourcePattern: String

  override protected def doPrepare(rc: RoundContext, prev: RDD[T]): Unit = {
    withCallSite(rc) {
      prev.foreachPartition { iter =>

        if (iter.hasNext) {
          doPreparePartition(rc) { (stageInfo, context, withOutput) =>

            val resourcePattern = stageInfo.resolveUserVariables(this.resourcePattern)
            val phAt = resourcePattern.lastIndexOf(Placeholder)
            require(phAt >= 0,
              "pattern does not contain any placeholder symbol " +
                s"(${Placeholder}): ${resourcePattern}")

            val resolvedPath = new JStringBuilder()
              .append(resourcePattern, 0, phAt)
              .append(context.getAttemptId)
              .append(resourcePattern, phAt + 1, resourcePattern.length)
              .toString

            withOutput(resolvedPath) { output =>
              var records = 0L
              while (iter.hasNext) {
                records += 1L
                output.write(iter.next())
              }
              records
            }
          }
        }
      }
    }
  }
}

abstract class DirectOutputPrepareGroup[T <: DataModel[T] with Writable: Manifest](
  setup: Action[Unit],
  prevs: Seq[(Source, BranchKey)])(
    partitioner: Partitioner)(
      implicit jobContext: JobContext)
  extends DirectOutputPrepare[T](setup, prevs) {
  self: CacheStrategy[RoundContext, Future[Unit]] =>

  def newDataModel(): T

  def outputPatternGenerator: OutputPatternGenerator[T]

  implicit def sortOrdering: SortOrdering

  def orderings(value: T): Seq[ValueOption[_]]

  def shuffleKey(value: T)(stageInfo: StageInfo): ShuffleKey = {
    new ShuffleKey(
      WritableSerDe.serialize(outputPatternGenerator.generate(value)(stageInfo)),
      WritableSerDe.serialize(orderings(value)))
  }

  override protected def doPrepare(rc: RoundContext, prev: RDD[T]): Unit = {
    withCallSite(rc) {
      prev.mapPartitions { iter =>
        val conf = rc.hadoopConf.value
        val stageInfo = StageInfo.deserialize(conf.get(StageInfo.KEY_NAME))

        iter.map(value => (shuffleKey(value)(stageInfo), WritableSerDe.serialize(value)))
      }
        .repartitionAndSortWithinPartitions(partitioner)
        .foreachPartition { iter =>

          if (iter.hasNext) {
            doPreparePartition(rc) { (stageInfo, context, withOutput) =>
              val buff = iter.buffered
              val pathOpt = new StringOption()
              val data = newDataModel()

              while (buff.hasNext) {
                val currentPath = buff.head._1.grouping
                WritableSerDe.deserialize(buff.head._1.grouping, pathOpt)
                withOutput(pathOpt.getAsString) { output =>
                  var records = 0L
                  while (buff.hasNext && Arrays.equals(buff.head._1.grouping, currentPath)) {
                    records += 1L
                    WritableSerDe.deserialize(buff.next()._2, data)
                    output.write(data)
                  }
                  records
                }
              }
            }
          }
        }
    }
  }
}
