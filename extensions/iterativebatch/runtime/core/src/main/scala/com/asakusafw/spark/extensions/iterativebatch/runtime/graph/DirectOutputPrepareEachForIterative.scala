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

import java.lang.{ StringBuilder => JStringBuilder }

import scala.annotation.meta.param
import scala.concurrent.{ ExecutionContext, Future }
import scala.reflect.ClassTag

import org.apache.hadoop.io.Writable
import org.apache.spark.{ Partitioner, ShuffleDependency, SparkContext, TaskContext }
import org.apache.spark.rdd.{ RDD, ShuffledRDD }
import org.slf4j.LoggerFactory

import org.apache.spark.backdoor._
import org.apache.spark.util.backdoor._

import com.asakusafw.bridge.stage.StageInfo
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.runtime.value.{ StringOption, ValueOption }
import com.asakusafw.spark.runtime.RoundContext
import com.asakusafw.spark.runtime.directio._
import com.asakusafw.spark.runtime.graph._
import com.asakusafw.spark.runtime.io.WritableSerDe
import com.asakusafw.spark.runtime.rdd._

abstract class DirectOutputPrepareEachForIterative[T <: DataModel[T] with Writable: ClassTag](
  @(transient @param) prevs: Seq[(Source, BranchKey)])(
    implicit @transient val sc: SparkContext) extends Action[RDD[(ShuffleKey, T)]] {
  self: CacheStrategy[RoundContext, Future[RDD[(ShuffleKey, T)]]] =>

  def newDataModel(): T

  override protected def doPerform(
    rc: RoundContext)(implicit ec: ExecutionContext): Future[RDD[(ShuffleKey, T)]] = {

    val rdds = prevs.map {
      case (source, branchKey) =>
        source.compute(rc).apply(branchKey).map(_.asInstanceOf[RDD[(_, _)]])
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
          sc.union(zipped)
        }
      }
    })
      .map(_.map(_._2.asInstanceOf[T]))
      .flatMap { prev =>

        sc.clearCallSite()
        sc.setCallSite(
          CallSite(rc.roundId.map(r => s"${label}: [${r}]").getOrElse(label), rc.toString))

        val rdd = doPrepare(rc, prev)

        val shuffleDep = rdd.dependencies.head
          .asInstanceOf[ShuffleDependency[ShuffleKey, Array[Byte], Array[Byte]]]

        sc.submitMapStage(shuffleDep).map { _ =>

          rdd.mapPartitions(iter => {
            val value = newDataModel()
            iter.map {
              case (key, bytes) =>
                WritableSerDe.deserialize(bytes, value)
                (key, value)
            }
          }, preservesPartitioning = true)
        }
      }
  }

  protected def doPrepare(
    rc: RoundContext, prev: RDD[T])(
      implicit ec: ExecutionContext): ShuffledRDD[ShuffleKey, Array[Byte], Array[Byte]]

  def confluent(rdds: Seq[RDD[(ShuffleKey, T)]]): RDD[(ShuffleKey, T)]
}

abstract class DirectOutputPrepareFlatEachForIterative[T <: DataModel[T] with Writable: ClassTag](
  prevs: Seq[(Source, BranchKey)])(
    implicit sc: SparkContext)
  extends DirectOutputPrepareEachForIterative[T](prevs) {
  self: CacheStrategy[RoundContext, Future[RDD[(ShuffleKey, T)]]] =>

  private val Placeholder: String = "*"

  def basePath: String

  def resourcePattern: String

  override protected def doPrepare(
    rc: RoundContext, prev: RDD[T])(
      implicit ec: ExecutionContext): ShuffledRDD[ShuffleKey, Array[Byte], Array[Byte]] = {

    val prepared = prev.mapPartitions { iter =>
      val conf = rc.hadoopConf.value
      val stageInfo = StageInfo.deserialize(conf.get(StageInfo.KEY_NAME))

      val basePathOpt = new StringOption()
      basePathOpt.modify(stageInfo.resolveUserVariables(this.basePath))

      val resourcePattern = stageInfo.resolveUserVariables(this.resourcePattern)
      val phAt = resourcePattern.lastIndexOf(Placeholder)
      require(phAt >= 0,
        "pattern does not contain any placeholder symbol " +
          s"(${Placeholder}): ${resourcePattern}")

      val taskContext = TaskContext.get
      val resolvedPath = new JStringBuilder()
        .append(resourcePattern, 0, phAt)
        .append(s"s${taskContext.stageId}-p${taskContext.partitionId}")
        .append(resourcePattern, phAt + 1, resourcePattern.length)
        .toString
      val pathOpt = new StringOption()
      pathOpt.modify(resolvedPath)
      val shuffleKey = new ShuffleKey(WritableSerDe.serialize(Seq(basePathOpt, pathOpt)))

      iter.map { value =>
        (shuffleKey, WritableSerDe.serialize(value))
      }
    }

    new ShuffledRDD[ShuffleKey, Array[Byte], Array[Byte]](
      prepared, IdentityPartitioner(prepared.partitions.length))
  }

  override def confluent(rdds: Seq[RDD[(ShuffleKey, T)]]): RDD[(ShuffleKey, T)] = {
    val zipped =
      rdds.groupBy(_.partitions.length).map {
        case (_, rdds) =>
          rdds.reduce(_.zipPartitions(_, preservesPartitioning = false)(_ ++ _))
      }.toSeq
    if (zipped.size == 1) {
      zipped.head
    } else {
      sc.union(zipped)
    }
  }
}

abstract class DirectOutputPrepareGroupEachForIterative[T <: DataModel[T] with Writable: ClassTag](
  prevs: Seq[(Source, BranchKey)])(
    val partitioner: Partitioner)(
      implicit sc: SparkContext)
  extends DirectOutputPrepareEachForIterative[T](prevs) {
  self: CacheStrategy[RoundContext, Future[RDD[(ShuffleKey, T)]]] =>

  def basePath: String

  def outputPatternGenerator: OutputPatternGenerator[T]

  implicit def sortOrdering: SortOrdering

  def orderings(value: T): Seq[ValueOption[_]]

  def shuffleKey(basePathOpt: StringOption, value: T): ShuffleKey = {
    new ShuffleKey(
      WritableSerDe.serialize(Seq(basePathOpt, outputPatternGenerator.generate(value))),
      WritableSerDe.serialize(orderings(value)))
  }

  override protected def doPrepare(
    rc: RoundContext, prev: RDD[T])(
      implicit ec: ExecutionContext): ShuffledRDD[ShuffleKey, Array[Byte], Array[Byte]] = {

    prev.mapPartitions { iter =>
      val conf = rc.hadoopConf.value
      val stageInfo = StageInfo.deserialize(conf.get(StageInfo.KEY_NAME))

      val basePathOpt = new StringOption()
      basePathOpt.modify(stageInfo.resolveUserVariables(this.basePath))

      iter.map { value =>
        (shuffleKey(basePathOpt, value), WritableSerDe.serialize(value))
      }
    }
      .repartitionAndSortWithinPartitions(partitioner)
      .asInstanceOf[ShuffledRDD[ShuffleKey, Array[Byte], Array[Byte]]]
  }

  override def confluent(rdds: Seq[RDD[(ShuffleKey, T)]]): RDD[(ShuffleKey, T)] = {
    sc.confluent(rdds, partitioner, Some(sortOrdering))
  }
}
