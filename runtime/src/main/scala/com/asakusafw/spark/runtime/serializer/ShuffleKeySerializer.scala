package com.asakusafw.spark.runtime.serializer

import com.esotericsoftware.kryo._
import com.esotericsoftware.kryo.io._

import com.asakusafw.runtime.value.ValueOption
import com.asakusafw.spark.runtime.driver.ShuffleKey

class ShuffleKeySerializer extends Serializer[ShuffleKey](false, false) {

  override def write(kryo: Kryo, output: Output, obj: ShuffleKey) = {
    kryo.writeClassAndObject(output, obj.grouping)
    kryo.writeClassAndObject(output, obj.ordering)
  }

  override def read(kryo: Kryo, input: Input, t: Class[ShuffleKey]): ShuffleKey = {
    new ShuffleKey(
      kryo.readClassAndObject(input).asInstanceOf[Seq[_ <: ValueOption[_]]],
      kryo.readClassAndObject(input).asInstanceOf[Seq[_ <: ValueOption[_]]])
  }
}

object ShuffleKeySerializer extends ShuffleKeySerializer
