package com.asakusafw.spark.runtime.driver

import scala.collection.mutable
import scala.concurrent.{ Await, Future }
import scala.concurrent.duration.Duration

import org.apache.hadoop.conf.Configuration
import org.apache.spark.Partitioner
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import com.asakusafw.runtime.model.DataModel
import com.asakusafw.spark.runtime.aggregation.Aggregation
import com.asakusafw.spark.runtime.fragment._
import com.asakusafw.spark.runtime.io._
import com.asakusafw.spark.runtime.rdd._

trait Branching[T] {
  this: SubPlanDriver =>

  def branchKeys: Set[BranchKey]

  def partitioners: Map[BranchKey, Option[Partitioner]]

  def orderings: Map[BranchKey, Ordering[ShuffleKey]]

  def aggregations: Map[BranchKey, Aggregation[ShuffleKey, _, _]]

  def shuffleKey(branch: BranchKey, value: Any): ShuffleKey

  def serialize(branch: BranchKey, value: Any): Array[Byte]

  def deserialize(branch: BranchKey, value: Array[Byte]): Any

  def fragments(broadcasts: Map[BroadcastId, Broadcast[_]]): (Fragment[T], Map[BranchKey, OutputFragment[_]])

  def branch(rdd: RDD[(_, T)]): Map[BranchKey, RDD[(ShuffleKey, _)]] = {
    val broadcasts = this.broadcasts.map {
      case (key, future) => key -> Await.result(future, Duration.Inf)
    }.toMap[BroadcastId, Broadcast[_]]
    if (branchKeys.size == 1 && partitioners.size == 0) {
      Map(branchKeys.head ->
        rdd.mapPartitions({ iter =>
          f(iter, broadcasts).map {
            case (Branch(_, k), v) => (k, v)
          }
        }, preservesPartitioning = true))
    } else {
      rdd.branch[ShuffleKey, Array[Byte]](
        branchKeys,
        { iter =>
          val combiners = aggregations.collect {
            case (b, agg) if agg.mapSideCombine => b -> agg.valueCombiner()
          }.toMap[BranchKey, Aggregation.Combiner[ShuffleKey, _, _]]

          if (combiners.isEmpty) {
            f(iter, broadcasts).map {
              case (bk @ Branch(b, _), value) => (bk, serialize(b, value))
            }
          } else {
            f(iter, broadcasts).flatMap {
              case (Branch(b, k), v) if combiners.contains(b) =>
                combiners(b).asInstanceOf[Aggregation.Combiner[ShuffleKey, Any, Any]]
                  .insert(k, v)
                Iterator.empty
              case (bk @ Branch(b, _), v) => Iterator((bk, serialize(b, v)))
            } ++ combiners.iterator.flatMap {
              case (b, combiner) =>
                combiner.iterator.map {
                  case (k, v) => (Branch(b, k), serialize(b, v))
                }
            }
          }
        },
        partitioners = partitioners.map {
          case (branchKey, Some(part)) => branchKey -> part
          case (branchKey, None)       => branchKey -> IdentityPartitioner(rdd.partitions.length)
        },
        keyOrderings = orderings,
        preservesPartitioning = true).map {
          case (b, rdd) =>
            b -> rdd.mapPartitions({ iter =>
              iter.map {
                case (k, v) => (k, deserialize(b, v))
              }
            }, preservesPartitioning = true)
        }
    }
  }

  private def f(iter: Iterator[(_, T)], broadcasts: Map[BroadcastId, Broadcast[_]]): Iterator[(Branch[ShuffleKey], _)] = {
    val (fragment, outputs) = fragments(broadcasts)
    assert(outputs.keys.toSet == branchKeys,
      s"The branch keys of outputs and branch keys field should be the same: (${
        outputs.keys.mkString("(", ",", ")")
      }, ${
        branchKeys.mkString("(", ",", ")")
      })")
    val branchOutputs = outputs.toArray

    new ResourceBrokingIterator(
      hadoopConf.value,
      iter.flatMap {
        case (_, value) =>
          fragment.reset()
          fragment.add(value)
          branchOutputs.iterator.flatMap {
            case (key, output) =>
              output.iterator.map(value => (Branch(key, shuffleKey(key, value)), value))
          }
      })
  }
}
