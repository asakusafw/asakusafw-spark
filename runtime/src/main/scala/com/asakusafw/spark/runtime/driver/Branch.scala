package com.asakusafw.spark.runtime.driver

import scala.collection.mutable

import org.apache.hadoop.conf.Configuration
import org.apache.spark.Partitioner
import org.apache.spark.SparkContext._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import com.asakusafw.runtime.model.DataModel
import com.asakusafw.spark.runtime.aggregation.Aggregation
import com.asakusafw.spark.runtime.fragment._
import com.asakusafw.spark.runtime.rdd._

trait Branch[T] {

  def hadoopConf: Broadcast[Configuration]

  def branchKeys: Set[BranchKey]

  def partitioners: Map[BranchKey, Partitioner]

  def orderings: Map[BranchKey, Ordering[ShuffleKey]]

  def aggregations: Map[BranchKey, Aggregation[ShuffleKey, _, _]]

  def shuffleKey(branch: BranchKey, value: Any): ShuffleKey

  def fragments: (Fragment[T], Map[BranchKey, OutputFragment[_]])

  def branch(rdd: RDD[(_, T)]): Map[BranchKey, RDD[(ShuffleKey, _)]] = {
    if (branchKeys.size == 1 && partitioners.size == 0) {
      Map(branchKeys.head ->
        rdd.mapPartitions({ iter =>
          f(iter).map {
            case ((_, k), v) => (k, v)
          }
        }, preservesPartitioning = true))
    } else {
      rdd.branch[BranchKey, ShuffleKey, Any](
        branchKeys,
        { iter =>
          val combiners = aggregations.collect {
            case (b, agg) if agg.mapSideCombine => b -> agg.valueCombiner()
          }.toMap[BranchKey, Aggregation.Combiner[ShuffleKey, _, _]]

          if (combiners.isEmpty) {
            f(iter)
          } else {
            f(iter).flatMap {
              case ((b, k), v) if combiners.contains(b) =>
                combiners(b).asInstanceOf[Aggregation.Combiner[ShuffleKey, Any, Any]]
                  .insert(k, v)
                Iterator.empty
              case otherwise => Iterator(otherwise)
            } ++ combiners.iterator.flatMap {
              case (b, combiner) =>
                combiner.iterator.map {
                  case (k, v) => ((b, k), v)
                }
            }
          }
        },
        partitioners = partitioners,
        keyOrderings = orderings,
        preservesPartitioning = true)
    }
  }

  private def f(iter: Iterator[(_, T)]): Iterator[((BranchKey, ShuffleKey), _)] = {
    val (fragment, outputs) = fragments
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
              output.iterator.map(value => ((key, shuffleKey(key, value)), value))
          }
      })
  }
}
