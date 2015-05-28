package com.asakusafw.spark.runtime.io

case class BufferSlice(buff: Array[Byte], off: Int, len: Int)

object BufferSlice {

  def apply(buff: Array[Byte]): BufferSlice = {
    BufferSlice(buff, 0, buff.length)
  }
}
