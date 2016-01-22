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
package com.asakusafw.spark.runtime

import scala.collection.mutable
import scala.language.implicitConversions
import scala.reflect.ClassTag

import org.apache.spark.{ Partitioner, SparkContext }
import org.apache.spark.rdd.{ RDD, ShuffledRDD }

import org.apache.spark.backdoor._
import org.apache.spark.rdd.backdoor._

import com.asakusafw.spark.runtime.util.Iterators._

package object rdd {

  implicit def rddToBranchRDDFunctions[T](rdd: RDD[T]): BranchRDDFunctions[T] = {
    new BranchRDDFunctions(rdd)
  }

  implicit class AugmentedPairRDD[K, V](val rdd: RDD[(K, V)]) extends AnyVal {

    def shuffle(part: Partitioner, ordering: Option[Ordering[K]] = None)(
      implicit keyTag: ClassTag[K], valueTag: ClassTag[V]): RDD[(K, V)] = {
      if (rdd.partitioner == Some(part)) {
        rdd
      } else {
        implicit val ord = ordering.orNull
        rdd.repartitionAndSortWithinPartitions(part)
      }
    }
  }

  implicit class AugmentedSparkContext(val sc: SparkContext) extends AnyVal {

    def confluent[K: ClassTag, V: ClassTag](
      rdds: Seq[RDD[(K, V)]], part: Partitioner, ordering: Option[Ordering[K]]): RDD[(K, V)] = {

      if (rdds.nonEmpty) {
        ordering match {
          case Some(ord) =>
            rdds.map(_.shuffle(part, ordering)).reduceLeft { (left, right) =>
              left.zipPartitions(right, preservesPartitioning = true) {
                case (leftIter, rightIter) if leftIter.hasNext && rightIter.hasNext =>
                  leftIter.sortmerge(rightIter)(ord)
                case (leftIter, _) if leftIter.hasNext => leftIter
                case (_, rightIter) => rightIter
              }
            }
          case None =>
            rdds.map(_.shuffle(part, ordering)).reduceLeft { (left, right) =>
              left.zipPartitions(right, preservesPartitioning = true)(_ ++ _)
            }
        }
      } else {
        sc.emptyRDD
      }
    }

    def smcogroup[K: ClassTag](
      rdds: Seq[(RDD[(K, _)], Option[Ordering[K]])],
      part: Partitioner,
      grouping: Ordering[K]): RDD[(K, Seq[Iterator[_]])] = {

      if (rdds.nonEmpty) {
        val ord = Option(grouping)
        val shuffle: ((RDD[(K, Any)], Option[Ordering[K]])) => RDD[(K, _)] = {
          case (rdd, o) => rdd.shuffle(part, o.orElse(ord))
        }

        val grouped = rdds.map(shuffle).map { shuffled =>
          shuffled.mapPartitions(
            _.asInstanceOf[Iterator[(K, Any)]].groupByOrderedKey()(grouping),
            preservesPartitioning = true)
        }

        sequence(grouped)(grouping)
      } else {
        sc.emptyRDD
      }
    }
  }

  private def sequence[K: Ordering](
    rdds: Seq[RDD[(K, Iterator[_])]]): RDD[(K, Seq[Iterator[_]])] = {
    assert(rdds.size > 0)

    (rdds.head.map { case (key, iter) => key -> Seq(iter) } /: rdds.tail.zipWithIndex) {
      case (acc, (rdd, i)) =>
        acc.zipPartitions(rdd, preservesPartitioning = true) { (leftIter, rightIter) =>
          val ord = implicitly[Ordering[K]]
          val leftBuff = leftIter.buffered
          val rightBuff = rightIter.buffered

          new Iterator[(K, Seq[Iterator[_]])] {

            override def hasNext: Boolean = leftBuff.hasNext || rightBuff.hasNext

            override def next(): (K, Seq[Iterator[_]]) = {
              (leftBuff.hasNext, rightBuff.hasNext) match {
                case (true, true) =>
                  val key = ord.min(leftBuff.head._1, rightBuff.head._1)
                  key -> ((if (ord.equiv(key, leftBuff.head._1)) {
                    leftBuff.next()._2
                  } else {
                    Seq.fill(i + 1)(Iterator.empty)
                  }) :+ (if (ord.equiv(key, rightBuff.head._1)) {
                    rightBuff.next()._2
                  } else {
                    Iterator.empty
                  }))
                case (true, false) =>
                  leftBuff.head._1 -> (leftBuff.next()._2 :+ Iterator.empty)
                case (false, true) =>
                  rightBuff.head._1 -> (Seq.fill(i + 1)(Iterator.empty) :+ rightBuff.next()._2)
                case _ =>
                  throw new AssertionError()
              }
            }
          }
        }
    }
  }
}
