/*
 * Copyright 2011-2021 Asakusa Framework Team.
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
package rdd

import java.util.Arrays

import com.asakusafw.runtime.io.util.WritableRawComparable

class ShuffleKey(
  val grouping: Array[Byte],
  val ordering: Array[Byte]) extends Equals {

  def this(grouping: Array[Byte]) = this(grouping, Array.emptyByteArray)

  def this() = this(Array.emptyByteArray)

  override def hashCode: Int = Arrays.hashCode(grouping)

  override def equals(obj: Any): Boolean = {
    obj match {
      case that: ShuffleKey =>
        (that canEqual this) &&
          Arrays.equals(this.grouping, that.grouping) &&
          Arrays.equals(this.ordering, that.ordering)
      case _ => false
    }
  }

  override def canEqual(obj: Any): Boolean = {
    obj.isInstanceOf[ShuffleKey]
  }

  def dropOrdering: ShuffleKey = new ShuffleKey(grouping)
}

object ShuffleKey {

  val empty: ShuffleKey = new ShuffleKey()
}
