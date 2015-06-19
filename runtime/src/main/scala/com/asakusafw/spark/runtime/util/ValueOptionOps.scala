/*
 * Copyright 2011-2015 Asakusa Framework Team.
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
package com.asakusafw.spark.runtime.util

import com.asakusafw.runtime.value._
import com.asakusafw.spark.runtime.orderings._

object ValueOptionOps {

  def copy(from: BooleanOption, to: BooleanOption): Unit = {
    to.copyFrom(from)
  }

  def copy(from: ByteOption, to: ByteOption): Unit = {
    to.copyFrom(from)
  }

  def copy(from: ShortOption, to: ShortOption): Unit = {
    to.copyFrom(from)
  }

  def copy(from: IntOption, to: IntOption): Unit = {
    to.copyFrom(from)
  }

  def copy(from: LongOption, to: LongOption): Unit = {
    to.copyFrom(from)
  }

  def copy(from: FloatOption, to: FloatOption): Unit = {
    to.copyFrom(from)
  }

  def copy(from: DoubleOption, to: DoubleOption): Unit = {
    to.copyFrom(from)
  }

  def copy(from: DecimalOption, to: DecimalOption): Unit = {
    to.copyFrom(from)
  }

  def copy(from: StringOption, to: StringOption): Unit = {
    to.copyFrom(from)
  }

  def copy(from: DateOption, to: DateOption): Unit = {
    to.copyFrom(from)
  }

  def copy(from: DateTimeOption, to: DateTimeOption): Unit = {
    to.copyFrom(from)
  }

  def setZero(to: LongOption): Unit = {
    to.modify(0)
  }

  def setZero(to: DoubleOption): Unit = {
    to.modify(0)
  }

  def setZero(to: DecimalOption): Unit = {
    to.modify(BigDecimal(0).underlying)
  }

  def inc(to: LongOption): Unit = {
    to.add(1L)
  }

  def add(value: ByteOption, acc: LongOption): Unit = {
    acc.add(value.get)
  }

  def add(value: ShortOption, acc: LongOption): Unit = {
    acc.add(value.get)
  }

  def add(value: IntOption, acc: LongOption): Unit = {
    acc.add(value.get)
  }

  def add(value: LongOption, acc: LongOption): Unit = {
    acc.add(value.get)
  }

  def add(value: FloatOption, acc: DoubleOption): Unit = {
    acc.add(value.get)
  }

  def add(value: DoubleOption, acc: DoubleOption): Unit = {
    acc.add(value.get)
  }

  def add(value: DecimalOption, acc: DecimalOption): Unit = {
    acc.add(value.get)
  }

  def max(combiner: BooleanOption, value: BooleanOption): Unit = {
    if (combiner.isNull || BooleanOptionOrdering.compare(combiner, value) < 0) {
      combiner.modify(value.get)
    }
  }

  def max(combiner: ByteOption, value: ByteOption): Unit = {
    if (combiner.isNull || ByteOptionOrdering.compare(combiner, value) < 0) {
      combiner.modify(value.get)
    }
  }

  def max(combiner: ShortOption, value: ShortOption): Unit = {
    if (combiner.isNull || ShortOptionOrdering.compare(combiner, value) < 0) {
      combiner.modify(value.get)
    }
  }

  def max(combiner: IntOption, value: IntOption): Unit = {
    if (combiner.isNull || IntOptionOrdering.compare(combiner, value) < 0) {
      combiner.modify(value.get)
    }
  }

  def max(combiner: LongOption, value: LongOption): Unit = {
    if (combiner.isNull || LongOptionOrdering.compare(combiner, value) < 0) {
      combiner.modify(value.get)
    }
  }

  def max(combiner: FloatOption, value: FloatOption): Unit = {
    if (combiner.isNull || FloatOptionOrdering.compare(combiner, value) < 0) {
      combiner.modify(value.get)
    }
  }

  def max(combiner: DoubleOption, value: DoubleOption): Unit = {
    if (combiner.isNull || DoubleOptionOrdering.compare(combiner, value) < 0) {
      combiner.modify(value.get)
    }
  }

  def max(combiner: DecimalOption, value: DecimalOption): Unit = {
    if (combiner.isNull || DecimalOptionOrdering.compare(combiner, value) < 0) {
      combiner.modify(value.get)
    }
  }

  def max(combiner: StringOption, value: StringOption): Unit = {
    if (combiner.isNull || StringOptionOrdering.compare(combiner, value) < 0) {
      combiner.modify(value.get)
    }
  }

  def max(combiner: DateOption, value: DateOption): Unit = {
    if (combiner.isNull || DateOptionOrdering.compare(combiner, value) < 0) {
      combiner.modify(value.get)
    }
  }

  def max(combiner: DateTimeOption, value: DateTimeOption): Unit = {
    if (combiner.isNull || DateTimeOptionOrdering.compare(combiner, value) < 0) {
      combiner.modify(value.get)
    }
  }

  def min(combiner: BooleanOption, value: BooleanOption): Unit = {
    if (combiner.isNull || BooleanOptionOrdering.compare(combiner, value) > 0) {
      combiner.modify(value.get)
    }
  }

  def min(combiner: ByteOption, value: ByteOption): Unit = {
    if (combiner.isNull || ByteOptionOrdering.compare(combiner, value) > 0) {
      combiner.modify(value.get)
    }
  }

  def min(combiner: ShortOption, value: ShortOption): Unit = {
    if (combiner.isNull || ShortOptionOrdering.compare(combiner, value) > 0) {
      combiner.modify(value.get)
    }
  }

  def min(combiner: IntOption, value: IntOption): Unit = {
    if (combiner.isNull || IntOptionOrdering.compare(combiner, value) > 0) {
      combiner.modify(value.get)
    }
  }

  def min(combiner: LongOption, value: LongOption): Unit = {
    if (combiner.isNull || LongOptionOrdering.compare(combiner, value) > 0) {
      combiner.modify(value.get)
    }
  }

  def min(combiner: FloatOption, value: FloatOption): Unit = {
    if (combiner.isNull || FloatOptionOrdering.compare(combiner, value) > 0) {
      combiner.modify(value.get)
    }
  }

  def min(combiner: DoubleOption, value: DoubleOption): Unit = {
    if (combiner.isNull || DoubleOptionOrdering.compare(combiner, value) > 0) {
      combiner.modify(value.get)
    }
  }

  def min(combiner: DecimalOption, value: DecimalOption): Unit = {
    if (combiner.isNull || DecimalOptionOrdering.compare(combiner, value) > 0) {
      combiner.modify(value.get)
    }
  }

  def min(combiner: StringOption, value: StringOption): Unit = {
    if (combiner.isNull || StringOptionOrdering.compare(combiner, value) > 0) {
      combiner.modify(value.get)
    }
  }

  def min(combiner: DateOption, value: DateOption): Unit = {
    if (combiner.isNull || DateOptionOrdering.compare(combiner, value) > 0) {
      combiner.modify(value.get)
    }
  }

  def min(combiner: DateTimeOption, value: DateTimeOption): Unit = {
    if (combiner.isNull || DateTimeOptionOrdering.compare(combiner, value) > 0) {
      combiner.modify(value.get)
    }
  }
}
