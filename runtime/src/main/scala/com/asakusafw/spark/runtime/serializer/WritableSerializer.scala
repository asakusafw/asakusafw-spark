package com.asakusafw.spark.runtime.serializer

import java.io.{ DataInputStream, DataOutputStream }

import org.apache.hadoop.io.Writable

import com.esotericsoftware.kryo._
import com.esotericsoftware.kryo.io._

import com.asakusafw.runtime.model._

abstract class WritableSerializer[W <: Writable] extends Serializer[W](false, false) {

  var dos: DataOutputStream = _
  var last: Output = _

  override def write(kryo: Kryo, output: Output, obj: W) = {
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

  override def read(kryo: Kryo, input: Input, t: Class[W]): W = {
    val obj = newInstance
    obj.readFields(new DataInputStream(input))
    obj
  }

  def newInstance: W
}
