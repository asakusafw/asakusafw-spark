package com.asakusafw.spark.runtime.serializer

import com.esotericsoftware.kryo._
import com.esotericsoftware.kryo.io._

import com.asakusafw.runtime.value.ValueOption
import com.asakusafw.spark.runtime.driver.ShuffleKey

class ShuffleKeySerializer extends Serializer[ShuffleKey](false, false) {

  override def write(kryo: Kryo, output: Output, obj: ShuffleKey) = {
    output.writeInt(obj.grouping.length)
    output.write(obj.grouping)
    output.writeInt(obj.ordering.length)
    output.write(obj.ordering)
  }

  override def read(kryo: Kryo, input: Input, t: Class[ShuffleKey]): ShuffleKey = {
    new ShuffleKey(
      input.readBytes(input.readInt()),
      input.readBytes(input.readInt()))
  }
}

object ShuffleKeySerializer extends ShuffleKeySerializer
