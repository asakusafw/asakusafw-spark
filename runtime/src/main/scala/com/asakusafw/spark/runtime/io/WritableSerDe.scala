package com.asakusafw.spark.runtime.io

import java.util.Arrays

import org.apache.hadoop.io.Writable

import com.asakusafw.runtime.io.util.DataBuffer

class WritableSerDe {

  private[this] val buffer = new DataBuffer()

  def serialize(value: Writable): Array[Byte] = {
    buffer.reset(0, 0)
    value.write(buffer)
    Arrays.copyOfRange(buffer.getData, buffer.getReadPosition, buffer.getReadLimit)
  }

  def serialize(values: Seq[Writable]): Array[Byte] = {
    buffer.reset(0, 0)
    values.foreach(_.write(buffer))
    Arrays.copyOfRange(buffer.getData, buffer.getReadPosition, buffer.getReadLimit)
  }

  def deserialize(arr: Array[Byte], value: Writable): Unit = {
    buffer.reset(arr, 0, arr.length)
    value.readFields(buffer)
  }

  def deserialize(arr: Array[Byte], values: Seq[Writable]): Unit = {
    buffer.reset(arr, 0, arr.length)
    values.foreach(_.readFields(buffer))
  }
}
