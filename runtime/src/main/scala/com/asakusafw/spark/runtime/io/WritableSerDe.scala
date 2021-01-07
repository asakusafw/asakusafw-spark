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
package com.asakusafw.spark.runtime.io

import java.util.Arrays

import org.apache.hadoop.io.Writable

import com.asakusafw.runtime.io.util.DataBuffer

class WritableSerDe {

  private[this] val buffer = new DataBuffer()

  def serialize(value: Writable): Array[Byte] = {
    serialize(Seq(value))
  }

  def serialize(values: Seq[Writable]): Array[Byte] = {
    buffer.reset(0, 0)
    values.foreach(_.write(buffer))
    Arrays.copyOfRange(buffer.getData, buffer.getReadPosition, buffer.getReadLimit)
  }

  def deserialize(bytes: Array[Byte], value: Writable): Unit = {
    deserialize(bytes, Seq(value))
  }

  def deserialize(bytes: Array[Byte], off: Int, value: Writable): Unit = {
    deserialize(bytes, off, Seq(value))
  }

  def deserialize(bytes: Array[Byte], values: Seq[Writable]): Unit = {
    deserialize(bytes, 0, values)
  }

  def deserialize(bytes: Array[Byte], off: Int, values: Seq[Writable]): Unit = {
    buffer.reset(bytes, off, bytes.length - off)
    values.foreach(_.readFields(buffer))
  }
}

object WritableSerDe {

  private[this] val serdes = new ThreadLocal[WritableSerDe]() {
    override def initialValue: WritableSerDe = new WritableSerDe()
  }

  def serialize(value: Writable): Array[Byte] = {
    serdes.get.serialize(value)
  }

  def serialize(values: Seq[Writable]): Array[Byte] = {
    serdes.get.serialize(values)
  }

  def deserialize(bytes: Array[Byte], value: Writable): Unit = {
    serdes.get.deserialize(bytes, value)
  }

  def deserialize(bytes: Array[Byte], off: Int, value: Writable): Unit = {
    serdes.get.deserialize(bytes, off, value)
  }

  def deserialize(bytes: Array[Byte], values: Seq[Writable]): Unit = {
    serdes.get.deserialize(bytes, values)
  }

  def deserialize(bytes: Array[Byte], off: Int, values: Seq[Writable]): Unit = {
    serdes.get.deserialize(bytes, off, values)
  }
}
