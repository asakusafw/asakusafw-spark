package com.asakusafw.spark.runtime.serializer

import com.esotericsoftware.kryo._
import com.esotericsoftware.kryo.io._

import com.asakusafw.runtime.value.ValueOption
import com.asakusafw.spark.runtime.driver.ShuffleKey

class ShuffleKeySerializer extends Serializer[ShuffleKey](false, false) {

  override def write(kryo: Kryo, output: Output, obj: ShuffleKey) = {
    output.writeInt(obj.grouping.length, true)
    output.write(obj.grouping)
    output.writeInt(obj.ordering.length, true)
    output.write(obj.ordering)
  }

  override def read(kryo: Kryo, input: Input, t: Class[ShuffleKey]): ShuffleKey = {
    new ShuffleKey(
      input.readBytes(input.readInt(true)),
      input.readBytes(input.readInt(true)))
  }
}

object ShuffleKeySerializer extends ShuffleKeySerializer
