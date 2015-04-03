package com.asakusafw.spark.runtime.serializer

import java.io.{ DataInputStream, DataOutputStream }

import com.esotericsoftware.kryo._
import com.esotericsoftware.kryo.io._

import com.asakusafw.runtime.value.ValueOption

abstract class ValueOptionSerializer[T <: ValueOption[T]] extends Serializer[T](false, false) {

  var dos: DataOutputStream = _
  var last: Output = _

  override def write(kryo: Kryo, output: Output, obj: T) = {
    if (output != last) {
      dos = new DataOutputStream(output)
      last = output
    }
    try {
      obj.write(dos)
    } finally {
      dos.flush()
    }
  }

  override def read(kryo: Kryo, input: Input, t: Class[T]): T = {
    val obj = newInstance
    obj.readFields(new DataInputStream(input))
    obj
  }

  def newInstance: T
}
