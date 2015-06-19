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

import scala.reflect.ClassTag

import com.asakusafw.runtime.core.Result
import com.asakusafw.runtime.model.DataModel

import org.apache.spark.util.collection.backdoor.CompactBuffer

abstract class OutputFragment[T <: DataModel[T]: ClassTag] extends Fragment[T] with Iterable[T] {

  def newDataModel(): T

  private[this] val buf = new CompactBuffer[T]
  private[this] var curSize = 0

  override def iterator: Iterator[T] = buf.iterator.take(curSize)

  override def add(result: T): Unit = {
    if (buf.size <= curSize) {
      buf += newDataModel()
    }
    buf(curSize).copyFrom(result)
    curSize += 1
  }

  override def reset(): Unit = {
    curSize = 0
  }
}
