package com.asakusafw.spark.runtime.io

import java.io.{ DataInputStream, DataOutputStream }
import java.nio.{ BufferOverflowException, ByteBuffer }

import scala.annotation.tailrec

import org.apache.hadoop.io.Writable

import com.asakusafw.runtime.io.util.DataBuffer

class WritableBuffer(bufferSize: Int, expansionFactor: Double) {

  def this(bufferSize: Int) = this(bufferSize, WritableBuffer.DefaultBufferExpansionFactor)

  def this() = this(WritableBuffer.DefaultBufferSize)

  private[this] var _buf: ByteBuffer = _

  private[this] def buffer: ByteBuffer = {
    if (_buf == null) {
      _buf = ByteBuffer.allocate(bufferSize)
    }
    _buf
  }

  private[this] val dos = new DataOutputStream(new ByteBufferOutputStream(buffer))
  private[this] val dis = new DataInputStream(new ByteBufferInputStream(buffer))

  def reset(buf: Array[Byte]): Unit = {
    _buf = ByteBuffer.wrap(buf)
  }

  def reset(buf: Array[Byte], off: Int, len: Int): Unit = {
    _buf = ByteBuffer.wrap(buf, off, len)
  }

  def put(value: Writable): this.type = {
    value.write(dos)
    this
  }

  def putAndSlice(value: Writable): BufferSlice = {
    @tailrec
    def putAndSlice0(): BufferSlice = {
      val buf = buffer
      val off = buf.position
      try {
        put(value)
        BufferSlice(buf.array, off, buf.position - off)
      } catch {
        case _: BufferOverflowException =>
          if (off == 0) {
            val buffer = new DataBuffer(
              (buf.capacity * expansionFactor).toInt + 1, expansionFactor)
            value.write(buffer)
            _buf = ByteBuffer.wrap(
              buffer.getData,
              buffer.getWritePosition,
              buffer.getData.length - buffer.getWritePosition)
            BufferSlice(buffer.getData, 0, buffer.getWritePosition)
          } else {
            _buf = ByteBuffer.allocate(math.max(bufferSize, buf.capacity))
            putAndSlice0()
          }
      }
    }
    putAndSlice0()
  }

  def get(value: Writable): this.type = {
    value.readFields(dis)
    this
  }

  def resetAndGet(slice: BufferSlice, value: Writable): this.type = {
    reset(slice.buff, slice.off, slice.len)
    get(value)
  }
}

object WritableBuffer {

  val DefaultBufferSize = 64 * 1024
  val DefaultBufferExpansionFactor = 1.5
}
