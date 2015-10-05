/*
 * Copyright 2011-2015 Asakusa Framework Team.
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
package com.asakusafw.yass.runtime
package flow

import org.apache.spark.Partitioner
import org.apache.spark.broadcast.{ Broadcast => Broadcasted }
import org.apache.spark.rdd.RDD

import com.asakusafw.spark.runtime.aggregation.Aggregation
import com.asakusafw.spark.runtime.driver.{ BroadcastId, ResourceBrokingIterator, ShuffleKey }
import com.asakusafw.spark.runtime.fragment.{ Fragment, OutputFragment }
import com.asakusafw.spark.runtime.rdd._

trait Branching[T] {

  def branchKeys: Set[BranchKey]

  def partitioners: Map[BranchKey, Option[Partitioner]]

  def orderings: Map[BranchKey, Ordering[ShuffleKey]]

  def aggregations: Map[BranchKey, Aggregation[ShuffleKey, _, _]]

  def shuffleKey(branch: BranchKey, value: Any): ShuffleKey

  def serialize(branch: BranchKey, value: Any): Array[Byte]

  def deserialize(branch: BranchKey, value: Array[Byte]): Any

  def fragments(
    broadcasts: Map[BroadcastId, Broadcasted[_]]): (Fragment[T], Map[BranchKey, OutputFragment[_]])

  def branch(
    rdd: RDD[(_, T)],
    broadcasts: Map[BroadcastId, Broadcasted[_]])(
      rc: RoundContext): Map[BranchKey, RDD[(ShuffleKey, _)]] = {
    if (branchKeys.size == 1 && partitioners.size == 0) {
      Map(branchKeys.head ->
        rdd.mapPartitions({ iter =>
          iterateFragments(iter, broadcasts)(rc).map {
            case (Branch(_, k), v) => (k, v)
          }
        }, preservesPartitioning = true))
    } else {
      rdd.branch[ShuffleKey, Array[Byte]](
        branchKeys,
        if (aggregations.values.exists(_.mapSideCombine)) {
          iterateWithCombiner(_, broadcasts)(rc)
        } else {
          iterateWithoutCombiner(_, broadcasts)(rc)
        },
        partitioners =
          partitioners.map {
            case (branchKey, Some(part)) => branchKey -> part
            case (branchKey, None) => branchKey -> IdentityPartitioner(rdd.partitions.length)
          },
        keyOrderings = orderings,
        preservesPartitioning = true)
        .map {
          case (b, rdd) =>
            b -> rdd.mapPartitions({ iter =>
              iter.map {
                case (k, v) => (k, deserialize(b, v))
              }
            }, preservesPartitioning = true)
        }
    }
  }

  private def iterateWithCombiner(
    iter: Iterator[(_, T)],
    broadcasts: Map[BroadcastId, Broadcasted[_]])(
      rc: RoundContext): Iterator[(Branch[ShuffleKey], Array[Byte])] = {
    val combiners = aggregations.collect {
      case (b, agg) if agg.mapSideCombine => b -> agg.valueCombiner()
    }.toMap[BranchKey, Aggregation.Combiner[ShuffleKey, _, _]]

    iterateFragments(iter, broadcasts)(rc).flatMap {
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

  private def iterateWithoutCombiner(
    iter: Iterator[(_, T)],
    broadcasts: Map[BroadcastId, Broadcasted[_]])(
      rc: RoundContext): Iterator[(Branch[ShuffleKey], Array[Byte])] = {
    iterateFragments(iter, broadcasts)(rc).map {
      case (bk @ Branch(b, _), value) => (bk, serialize(b, value))
    }
  }

  private def iterateFragments(
    iter: Iterator[(_, T)],
    broadcasts: Map[BroadcastId, Broadcasted[_]])(
      rc: RoundContext): Iterator[(Branch[ShuffleKey], _)] = {
    val (fragment, outputs) = fragments(broadcasts)
    assert(outputs.keys.toSet == branchKeys,
      s"The branch keys of outputs and branch keys field should be the same: (${
        outputs.keys.mkString("(", ",", ")")
      }, ${
        branchKeys.mkString("(", ",", ")")
      })")
    val branchOutputs = outputs.toArray

    new ResourceBrokingIterator(
      rc.hadoopConf.value,
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
