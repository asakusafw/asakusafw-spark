package com.asakusafw.spark.runtime.driver

import scala.collection.mutable
import scala.reflect.ClassTag

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._

import com.asakusafw.runtime.model.DataModel
import com.asakusafw.spark.runtime.fragment._
import com.asakusafw.spark.runtime.rdd._

abstract class CoGroupDriver[B, K](
  @transient val sc: SparkContext,
  @transient inputs: Seq[(RDD[(K, _)], Option[Ordering[K]])],
  @transient part: Partitioner,
  @transient grouping: Ordering[K])
    extends SubPlanDriver[B] {
  assert(inputs.size > 0)

  override def execute(): Map[B, RDD[(_, _)]] = {
    val cogroup = smcogroup[K](inputs, part, grouping)
    cogroup.branch[B, Any, Any](branchKeys, { iter =>
      val (fragment, outputs) = fragments
      assert(outputs.keys.toSet == branchKeys)
      iter.flatMap {
        case (_, iterables) =>
          fragment.reset()
          fragment.add(iterables)
          outputs.iterator.flatMap {
            case (key, output) =>
              def prepare[T <: DataModel[T]](buffer: mutable.ArrayBuffer[_]) = {
                buffer.asInstanceOf[mutable.ArrayBuffer[T]]
                  .map(out => ((key, shuffleKey(key, out)), out))
              }
              prepare(output.buffer)
          }
      }
    },
      partitioners = partitioners,
      keyOrderings = orderings,
      preservesPartitioning = true)
  }

  def fragments[T <: DataModel[T]]: (CoGroupFragment, Map[B, OutputFragment[T]])
}
