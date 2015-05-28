package com.asakusafw.spark.runtime.io

import java.io.OutputStream
import java.nio.ByteBuffer

class ByteBufferOutputStream(buffer: => ByteBuffer) extends OutputStream {

  override def write(b: Int): Unit = {
    buffer.put(b.toByte)
  }

  override def write(b: Array[Byte], off: Int, len: Int): Unit = {
    buffer.put(b, off, len)
  }
}
