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
package util

import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner

import com.asakusafw.spark.runtime.util.Iterators._

@RunWith(classOf[JUnitRunner])
class IteratorsSpecTest extends IteratorsSpec

class IteratorsSpec extends FlatSpec {

  behavior of Iterators.getClass.getSimpleName

  it should "groupByKey" in {
    val iter = Iterator((1, 10), (1, 11), (2, 20), (2, 21), (2, 22), (3, 30))

    val grouped = iter.groupByKey()

    assert(grouped.hasNext)
    val (key1, group1) = grouped.next()
    assert(key1 === 1)
    assert(group1.hasNext)
    assert(group1.next() === 10)
    assert(group1.next() === 11)
    assert(!group1.hasNext)

    assert(grouped.hasNext)
    val (key2, group2) = grouped.next()
    assert(key2 === 2)
    assert(group2.hasNext)
    assert(group2.next() === 20)

    // group2 has some more elements.
    assert(group2.hasNext)

    assert(grouped.hasNext)
    val (key3, group3) = grouped.next()
    assert(key3 === 3)
    assert(group3.hasNext)
    assert(group3.next() === 30)
    assert(!group3.hasNext)

    // the remains of group2 were dropped by `grouped.next()`.
    assert(!group2.hasNext)

    assert(!grouped.hasNext)
  }

  it should "sortmerge" in {
    val left = Iterator((1, 10), (1, 11), (2, 20), (4, 40))
    val right = Iterator((1, 12), (2, 21), (3, 30), (4, 41))

    assert(left.sortmerge(right).toSeq === Seq(
      (1, 10), (1, 11), (1, 12), (2, 20), (2, 21), (3, 30), (4, 40), (4, 41)))
  }
}
