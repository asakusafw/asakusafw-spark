package com.asakusafw.spark.runtime.io

import java.io.InputStream
import java.nio.ByteBuffer

class ByteBufferInputStream(buffer: => ByteBuffer) extends InputStream {

  override def read(): Int = {
    buffer.get() & 0xff
  }

  override def read(b: Array[Byte], off: Int, len: Int): Int = {
    val buf = buffer
    val pos = buf.position
    buf.get(b, off, len)
    buf.position - pos
  }

  override def available(): Int = {
    val buf = buffer
    buf.limit - buf.position
  }
}
