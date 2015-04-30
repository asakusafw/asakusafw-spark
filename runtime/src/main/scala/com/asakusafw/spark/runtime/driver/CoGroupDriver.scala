package com.asakusafw.spark.runtime.driver

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

import org.apache.hadoop.conf.Configuration
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd._

import org.apache.spark.backdoor._
import org.apache.spark.util.backdoor._
import com.asakusafw.spark.runtime.rdd._

abstract class CoGroupDriver(
  sc: SparkContext,
  hadoopConf: Broadcast[Configuration],
  broadcasts: Map[BroadcastId, Future[Broadcast[_]]],
  @transient prevs: Seq[(Seq[Future[RDD[(ShuffleKey, _)]]], Option[ShuffleKey.SortOrdering])],
  @transient grouping: ShuffleKey.GroupingOrdering,
  @transient part: Partitioner)
    extends SubPlanDriver(sc, hadoopConf, broadcasts) with Branching[Seq[Iterable[_]]] {
  assert(prevs.size > 0,
    s"Previous RDDs should be more than 0: ${prevs.size}")

  override def execute(): Map[BranchKey, Future[RDD[(ShuffleKey, _)]]] = {
    val future =
      ((prevs :\ Future.successful(List.empty[(Seq[RDD[(ShuffleKey, _)]], Option[ShuffleKey.SortOrdering])])) {
        case ((prevs, sort), list) =>
          ((prevs :\ Future.successful(List.empty[RDD[(ShuffleKey, _)]])) {
            case (prev, list) => prev.zip(list).map {
              case (p, l) => p :: l
            }
          }).map((_, sort)).zip(list).map {
            case (p, l) => p :: l
          }
      }).map { prevs =>
        sc.clearCallSite()
        sc.setCallSite(name)

        val cogrouped = smcogroup[ShuffleKey](
          prevs.map {
            case (rdds, sort) =>
              (confluent[ShuffleKey, Any](rdds, part, sort), sort)
          },
          part,
          grouping)
          .mapValues(_.toSeq).asInstanceOf[RDD[(ShuffleKey, Seq[Iterable[_]])]]

        branch(cogrouped.asInstanceOf[RDD[(_, Seq[Iterable[_]])]])
      }

    branchKeys.map(key => key -> future.map(_(key))).toMap
  }
}
