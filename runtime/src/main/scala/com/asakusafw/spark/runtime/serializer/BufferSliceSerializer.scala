package com.asakusafw.spark.runtime.serializer

import com.esotericsoftware.kryo._
import com.esotericsoftware.kryo.io._

import com.asakusafw.spark.runtime.io.BufferSlice

class BufferSliceSerializer extends Serializer[BufferSlice](false, false) {

  override def write(kryo: Kryo, output: Output, obj: BufferSlice) = {
    output.writeInt(obj.len)
    output.write(obj.buff, obj.off, obj.len)
  }

  override def read(kryo: Kryo, input: Input, t: Class[BufferSlice]): BufferSlice = {
    val len = input.readInt()
    val buff = Array.ofDim[Byte](len)
    input.read(buff)
    BufferSlice(buff)
  }
}

object BufferSliceSerializer extends BufferSliceSerializer
