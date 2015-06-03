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

  def deserialize(bytes: Array[Byte], value: Writable): Unit = {
    buffer.reset(bytes, 0, bytes.length)
    value.readFields(buffer)
  }

  def deserialize(bytes: Array[Byte], values: Seq[Writable]): Unit = {
    buffer.reset(bytes, 0, bytes.length)
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

  def deserialize(bytes: Array[Byte], values: Seq[Writable]): Unit = {
    serdes.get.deserialize(bytes, values)
  }
}
