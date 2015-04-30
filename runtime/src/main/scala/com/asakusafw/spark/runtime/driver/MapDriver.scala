package com.asakusafw.spark.runtime.driver

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

import org.apache.hadoop.conf.Configuration
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.broadcast.Broadcast

import org.apache.spark.backdoor._
import org.apache.spark.util.backdoor.CallSite
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.spark.runtime.rdd.BranchKey

abstract class MapDriver[T](
  sc: SparkContext,
  hadoopConf: Broadcast[Configuration],
  broadcasts: Map[BroadcastId, Future[Broadcast[_]]],
  @transient prevs: Seq[Future[RDD[(_, T)]]])
    extends SubPlanDriver(sc, hadoopConf, broadcasts) with Branching[T] {
  assert(prevs.size > 0,
    s"Previous RDDs should be more than 0: ${prevs.size}")

  override def execute(): Map[BranchKey, Future[RDD[(ShuffleKey, _)]]] = {

    val future = (if (prevs.size == 1) {
      prevs.head
    } else {
      ((prevs :\ Future.successful(List.empty[RDD[(_, T)]])) {
        case (prev, list) => prev.zip(list).map {
          case (p, l) => p :: l
        }
      }).map(new UnionRDD(sc, _).coalesce(sc.defaultParallelism, shuffle = false))
    }).map { prev =>
      sc.clearCallSite()
      sc.setCallSite(name)

      branch(prev)
    }

    branchKeys.map(key => key -> future.map(_(key))).toMap
  }
}
