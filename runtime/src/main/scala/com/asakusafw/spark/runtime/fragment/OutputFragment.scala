/*
 * Copyright 2011-2018 Asakusa Framework Team.
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

import com.asakusafw.runtime.flow.{ ArrayListBuffer, FileMapListBuffer, ListBuffer }
import com.asakusafw.runtime.model.DataModel

abstract class OutputFragment[T <: DataModel[T] with Writable](bufferSize: Int)
  extends Fragment[T] {

  def this() = this(-1)

  def newDataModel(): T

  private[this] val buf: ListBuffer[T] = {
    val buf =
      if (bufferSize >= 0) {
        new FileMapListBuffer[T](bufferSize)
      } else {
        new ArrayListBuffer[T]()
      }
    buf.begin()
    buf
  }

  override def doReset(): Unit = {
    buf.shrink()
    buf.begin()
  }

  override def doAdd(result: T): Unit = {
    if (buf.isExpandRequired()) {
      buf.expand(newDataModel())
    }
    buf.advance().copyFrom(result)
  }

  def iterator: Iterator[T] = {
    buf.end()

    val iter = buf.iterator()

    new Iterator[T] {

      private[this] var hasnext = true

      override def hasNext: Boolean = {
        if (hasnext) {
          if (!iter.hasNext) {
            hasnext = false
            doReset()
          }
        }
        hasnext
      }

      override def next(): T =
        if (hasNext) {
          iter.next()
        } else {
          Iterator.empty.next()
        }
    }
  }
}
