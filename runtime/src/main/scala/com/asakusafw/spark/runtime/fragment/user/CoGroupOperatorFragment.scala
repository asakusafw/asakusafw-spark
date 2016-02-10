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
package com.asakusafw.spark.runtime.fragment
package user

import com.asakusafw.runtime.core.Result
import com.asakusafw.runtime.flow.ListBuffer
import com.asakusafw.runtime.model.DataModel

abstract class CoGroupOperatorFragment(
  buffers: IndexedSeq[ListBuffer[_ <: DataModel[_]]], children: IndexedSeq[Fragment[_]])
  extends Fragment[IndexedSeq[Iterator[_]]] {

  def newDataModelFor[T <: DataModel[T]](i: Int): T

  override def doAdd(result: IndexedSeq[Iterator[_]]): Unit = {
    buffers.zip(result).zipWithIndex.foreach {
      case ((buffer, iter), i) =>
        def prepare[T <: DataModel[T]]() = {
          val buf = buffer.asInstanceOf[ListBuffer[T]]
          buf.begin()
          iter.foreach { input =>
            if (buf.isExpandRequired()) {
              buf.expand(newDataModelFor[T](i))
            }
            buf.advance().copyFrom(input.asInstanceOf[T])
          }
          buf.end()
        }
        prepare()
    }

    cogroup(buffers, children)

    buffers.foreach(_.shrink())
  }

  def cogroup(inputs: IndexedSeq[ListBuffer[_]], outputs: IndexedSeq[Result[_]]): Unit

  override def doReset(): Unit = {
    children.foreach(_.reset())
  }
}
