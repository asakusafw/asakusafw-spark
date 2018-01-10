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

import com.asakusafw.runtime.core.Result
import com.asakusafw.runtime.model.DataModel

abstract class EdgeFragment[T <: DataModel[T]](children: Array[Fragment[T]]) extends Fragment[T] {
  assert(children.size > 1,
    s"The size of children should be greater than 1: ${children.size}")

  def newDataModel(): T

  private[this] val dataModel: T = newDataModel()

  private[this] val size = children.length

  override def doAdd(result: T): Unit = {
    var i = 0
    while (i < size - 1) {
      dataModel.copyFrom(result)
      children(i).add(dataModel)
      i += 1
    }
    children(i).add(result)
  }

  override def doReset(): Unit = {
    var i = 0
    while (i < size) {
      children(i).reset()
      i += 1
    }
  }
}
