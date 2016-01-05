/*
 * Copyright 2011-2016 Asakusa Framework Team.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.asakusafw.spark.runtime.serializer

import com.esotericsoftware.kryo._
import com.esotericsoftware.kryo.io._

import com.asakusafw.runtime.value.ValueOption
import com.asakusafw.spark.runtime.rdd.ShuffleKey

class ShuffleKeySerializer extends Serializer[ShuffleKey](false, false) {

  override def write(kryo: Kryo, output: Output, obj: ShuffleKey): Unit = {
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
