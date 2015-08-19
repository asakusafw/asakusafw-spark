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
package com.asakusafw.spark.runtime.fragment

import org.apache.hadoop.io.Writable

import com.asakusafw.runtime.flow.{ FileMapListBuffer, ListBuffer }
import com.asakusafw.runtime.model.DataModel

abstract class OutputFragment[T <: DataModel[T] with Writable]
  extends Fragment[T] {

  def newDataModel(): T

  private[this] val buf: ListBuffer[T] = new FileMapListBuffer[T]
  reset()

  override def reset(): Unit = {
    buf.shrink()
    buf.begin()
  }

  override def add(result: T): Unit = {
    if (buf.isExpandRequired()) {
      buf.expand(newDataModel())
    }
    buf.advance().copyFrom(result)
  }

  def iterator: Iterator[T] = {
    buf.end()
    val iter = buf.iterator()
    new Iterator[T] {
      def hasNext: Boolean = {
        if (iter.hasNext) {
          true
        } else {
          reset()
          false
        }
      }
      def next(): T = iter.next()
    }
  }
}
