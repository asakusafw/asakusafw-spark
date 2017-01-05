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
package com.asakusafw.spark.runtime
package graph

import scala.collection.mutable
import scala.concurrent.{ Await, Future }
import scala.concurrent.duration.Duration

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Writable
import org.apache.spark.Partitioner
import org.apache.spark.broadcast.{ Broadcast => Broadcasted }
import org.apache.spark.rdd.RDD

import com.asakusafw.runtime.model.DataModel
import com.asakusafw.spark.runtime.aggregation.Aggregation
import com.asakusafw.spark.runtime.fragment.{ Fragment, OutputFragment }
import com.asakusafw.spark.runtime.io.WritableSerDe
import com.asakusafw.spark.runtime.rdd._

trait Branching[T] {

  def jobContext: JobContext

  def branchKeys: Set[BranchKey]

  def partitioners: Map[BranchKey, Option[Partitioner]]

  def orderings: Map[BranchKey, Ordering[ShuffleKey]]

  def aggregations(
    broadcasts: Map[BroadcastId, Broadcasted[_]]): Map[BranchKey, Aggregation[ShuffleKey, _, _]]

  def shuffleKey(branch: BranchKey, value: Any): ShuffleKey

  def deserializerFor(branch: BranchKey): Array[Byte] => Any

  def fragments(
    broadcasts: Map[BroadcastId, Broadcasted[_]])(
      fragmentBufferSize: Int): (Fragment[T], Map[BranchKey, OutputFragment[_]])

  def branch(
    rdd: RDD[(_, T)],
    broadcasts: Map[BroadcastId, Broadcasted[_]],
    hadoopConf: Broadcasted[Configuration])(
      fragmentBufferSize: Int): Map[BranchKey, () => RDD[(ShuffleKey, _)]] = {
    if (branchKeys.size == 1 && partitioners.size == 0) {
      Map(branchKeys.head -> {
        val mapped = rdd.mapPartitions({ iter =>
          new ResourceBrokingIterator(
            hadoopConf.value,
            iterateFragments(iter, broadcasts)(fragmentBufferSize).map {
              case (Branch(_, k), v) => (k, v)
            })
        }, preservesPartitioning = true)
        () => mapped
      })
    } else {
      rdd.branch[ShuffleKey, Array[Byte]](
        branchKeys,
        { iter =>
          new ResourceBrokingIterator(
            hadoopConf.value, {
              val fragmentsIter = iterateFragments(iter, broadcasts)(fragmentBufferSize)
              val aggs = aggregations(broadcasts)
              if (aggs.nonEmpty) {
                iterateWithCombiner(fragmentsIter, aggs)
              } else {
                iterateWithoutCombiner(fragmentsIter)
              }
            })
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
            b -> { () =>
              rdd.mapPartitions({ iter =>
                val deserializer = deserializerFor(b)
                iter.map {
                  case (k, v) => (k, deserializer(v))
                }
              }, preservesPartitioning = true)
            }
        }
    }
  }

  private def iterateWithCombiner(
    iter: Iterator[(Branch[ShuffleKey], _)],
    aggregations: Map[BranchKey, Aggregation[ShuffleKey, _, _]]): Iterator[(Branch[ShuffleKey], Array[Byte])] = { // scalastyle:ignore
    val combiners = aggregations.map {
      case (b, agg) => b -> agg.valueCombiner()
    }.toMap[BranchKey, Aggregation.Combiner[ShuffleKey, _, _]]

    iter.flatMap {
      case (Branch(b, k), v) if combiners.contains(b) =>
        combiners(b).asInstanceOf[Aggregation.Combiner[ShuffleKey, Any, Any]].insert(k, v)
        Iterator.empty
      case (bk, v) => Iterator((bk, WritableSerDe.serialize(v.asInstanceOf[Writable])))
    } ++ combiners.iterator.flatMap {
      case (b, combiner) =>
        combiner.iterator.map {
          case (k, v) => (Branch(b, k), WritableSerDe.serialize(v.asInstanceOf[Writable]))
        }
    }
  }

  private def iterateWithoutCombiner(
    iter: Iterator[(Branch[ShuffleKey], _)]): Iterator[(Branch[ShuffleKey], Array[Byte])] = {
    iter.map {
      case (bk, value) => (bk, WritableSerDe.serialize(value.asInstanceOf[Writable]))
    }
  }

  private def iterateFragments(
    iter: Iterator[(_, T)],
    broadcasts: Map[BroadcastId, Broadcasted[_]])(
      fragmentBufferSize: Int): Iterator[(Branch[ShuffleKey], _)] = {
    val (fragment, outputs) = fragments(broadcasts)(fragmentBufferSize)
    assert(outputs.keys.toSet == branchKeys,
      s"The branch keys of outputs and branch keys field should be the same: (${
        outputs.keys.mkString("(", ",", ")")
      }, ${
        branchKeys.mkString("(", ",", ")")
      })")
    iter.flatMap {
      case (_, value) =>
        fragment.reset()
        fragment.add(value)
        outputs.iterator.flatMap {
          case (key, output) =>
            output.iterator.map(value => (Branch(key, shuffleKey(key, value)), value))
        }
    }
  }
}
