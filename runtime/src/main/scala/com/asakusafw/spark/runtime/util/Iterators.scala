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
package com.asakusafw.spark.runtime.util

object Iterators {
  self =>

  implicit class AugmentedOrderedPairIterator[K, V](val iter: Iterator[(K, V)]) extends AnyVal {

    def groupByKey()(implicit ord: Ordering[K]): Iterator[(K, Iterator[V])] = {
      self.groupByKey[K, V](iter)
    }

    def sortmerge(other: Iterator[(K, V)])(implicit ord: Ordering[K]): Iterator[(K, V)] = {
      self.sortmerge(iter, other)
    }
  }

  def groupByKey[K: Ordering, V](
    iter: Iterator[(K, V)]): Iterator[(K, Iterator[V])] = {
    val ord = implicitly[Ordering[K]]
    val buff = iter.buffered

    new Iterator[(K, Iterator[V])] {

      var cur: Iterator[V] = Iterator.empty

      override def hasNext: Boolean = buff.hasNext

      override def next(): (K, Iterator[V]) = {
        while (cur.hasNext) cur.next()

        val key = buff.head._1

        cur = new Iterator[V] {

          override def hasNext: Boolean = buff.hasNext && ord.equiv(key, buff.head._1)

          override def next(): V = buff.next()._2
        }

        key -> cur
      }
    }
  }

  def sortmerge[K: Ordering, V](
    left: Iterator[(K, V)], right: Iterator[(K, V)]): Iterator[(K, V)] = {
    val ord = implicitly[Ordering[K]]
    val leftIterBuff = left.buffered
    val rightIterBuff = right.buffered

    new Iterator[(K, V)] {

      var curKey: K = ord.min(leftIterBuff.head._1, rightIterBuff.head._1)

      override def hasNext: Boolean = leftIterBuff.hasNext || rightIterBuff.hasNext

      override def next(): (K, V) = {
        if (leftIterBuff.hasNext && ord.equiv(curKey, leftIterBuff.head._1)) {
          leftIterBuff.next()
        } else if (rightIterBuff.hasNext && ord.equiv(curKey, rightIterBuff.head._1)) {
          rightIterBuff.next()
        } else {
          if (leftIterBuff.hasNext && rightIterBuff.hasNext) {
            curKey = ord.min(leftIterBuff.head._1, rightIterBuff.head._1)
            next()
          } else if (leftIterBuff.hasNext) {
            curKey = leftIterBuff.head._1
            leftIterBuff.next()
          } else {
            curKey = rightIterBuff.head._1
            rightIterBuff.next()
          }
        }
      }
    }
  }
}
