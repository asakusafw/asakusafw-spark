package com.asakusafw.spark.runtime.serializer

import com.esotericsoftware.kryo._
import com.esotericsoftware.kryo.io._

import com.asakusafw.runtime.value.ValueOption
import com.asakusafw.spark.runtime.rdd.{ Branch, BranchKey }

class BranchSerializer[K] extends Serializer[Branch[K]](false, false) {

  override def write(kryo: Kryo, output: Output, obj: Branch[K]) = {
    kryo.writeObject(output, obj.branchKey)
    kryo.writeClassAndObject(output, obj.actualKey)
  }

  override def read(kryo: Kryo, input: Input, t: Class[Branch[K]]): Branch[K] = {
    Branch(
      kryo.readObject(input, classOf[BranchKey]),
      kryo.readClassAndObject(input).asInstanceOf[K])
  }
}

object BranchSerializer extends BranchSerializer
