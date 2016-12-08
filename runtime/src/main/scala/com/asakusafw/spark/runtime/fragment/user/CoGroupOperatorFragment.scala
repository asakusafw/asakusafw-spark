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

import scala.collection.JavaConversions._

import java.lang.{ Iterable => JIterable }
import java.util.{ Iterator => JIterator }

import com.asakusafw.runtime.core.Result
import com.asakusafw.runtime.flow.ListBuffer
import com.asakusafw.runtime.model.DataModel

abstract class CoGroupOperatorFragment(
  buffers: IndexedSeq[Buffer[_ <: DataModel[_]]], children: IndexedSeq[Fragment[_]])
  extends Fragment[IndexedSeq[Iterator[_]]] {

  override def doAdd(result: IndexedSeq[Iterator[_]]): Unit = {
    try {
      buffers.zip(result).foreach {
        case (buffer, iter) =>
          def prepare[T <: DataModel[T]]() = {
            buffer.asInstanceOf[Buffer[T]]
              .prepare(iter.asInstanceOf[Iterator[T]])
          }
          prepare()
      }

      cogroup(buffers.map(_.iterable), children)
    } finally {
      buffers.foreach(_.cleanup())
    }
  }

  def cogroup(inputs: IndexedSeq[JIterable[_]], outputs: IndexedSeq[Result[_]]): Unit

  override def doReset(): Unit = {
    children.foreach(_.reset())
  }
}

trait Buffer[T <: DataModel[T]] {

  def prepare(iter: Iterator[T]): Unit

  def iterable: JIterable[T]

  def cleanup(): Unit
}

abstract class ListLikeBuffer[T <: DataModel[T]](buffer: ListBuffer[T]) extends Buffer[T] {

  def newDataModel(): T

  override def prepare(iter: Iterator[T]): Unit = {
    buffer.begin()
    iter.foreach { input =>
      if (buffer.isExpandRequired()) {
        buffer.expand(newDataModel())
      }
      buffer.advance().copyFrom(input.asInstanceOf[T])
    }
    buffer.end()
  }

  override def iterable: JIterable[T] = buffer

  override def cleanup(): Unit = buffer.shrink()
}

class IterableBuffer[T <: DataModel[T]] extends Buffer[T] {
  self =>

  private var iter: Iterator[T] = _

  def prepare(iter: Iterator[T]): Unit = {
    this.iter = iter
  }

  def iterable: JIterable[T] = new JIterable[T] {

    private var iter: JIterator[T] = self.iter

    override def iterator: JIterator[T] = {
      val result = iter
      if (result == null) { // scalastyle:ignore
        throw new IllegalStateException()
      }
      iter = null // scalastyle:ignore
      result
    }
  }

  def cleanup(): Unit = {
    if (iter != null) { // scalastyle:ignore
      while (iter.hasNext) {
        iter.next()
      }
      iter = null // scalastyle:ignore
    }
  }
}
