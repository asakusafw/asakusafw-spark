/*
 * Copyright 2011-2021 Asakusa Framework Team.
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

import scala.tools.cmd.Spec.Accumulator

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
    to.modify(java.math.BigDecimal.ZERO)
  }

  def inc(to: LongOption): Unit = {
    to.add(1L)
  }

  def copyAlways[V <: ValueOption[V]](accumulator: V, operand: V): Unit = {
    accumulator.copyFrom(operand)
  }

  def maxUnsafe[V <: ValueOption[V]](accumulator: V, operand: V): Unit = {
    accumulator.max(operand)
  }

  def minUnsafe[V <: ValueOption[V]](accumulator: V, operand: V): Unit = {
    accumulator.min(operand)
  }

  def addUnsafe(accumulator: LongOption, operand: ByteOption): Unit = {
    accumulator.add(operand.get())
  }

  def addUnsafe(accumulator: LongOption, operand: ShortOption): Unit = {
    accumulator.add(operand.get())
  }

  def addUnsafe(accumulator: LongOption, operand: IntOption): Unit = {
    accumulator.add(operand.get())
  }

  def addUnsafe(accumulator: LongOption, operand: LongOption): Unit = {
    accumulator.add(operand)
  }

  def addUnsafe(accumulator: DecimalOption, operand: DecimalOption): Unit = {
    accumulator.add(operand)
  }

  def addUnsafe(accumulator: DoubleOption, operand: FloatOption): Unit = {
    accumulator.add(operand.get())
  }

  def addUnsafe(accumulator: DoubleOption, operand: DoubleOption): Unit = {
    accumulator.add(operand)
  }

  def copyWithName[V <: ValueOption[V]](
    accumulator: V, operand: V, name: String, record: AnyRef): Unit = {
    checkNull(operand, name, record)
    accumulator.copyFrom(operand)
  }

  def maxWithName[V <: ValueOption[V]](
    accumulator: V, operand: V, name: String, record: AnyRef): Unit = {
    checkNull(operand, name, record)
    accumulator.max(operand)
  }

  def minWithName[V <: ValueOption[V]](
    accumulator: V, operand: V, name: String, record: AnyRef): Unit = {
    checkNull(operand, name, record)
    accumulator.min(operand)
  }

  def addWithName(
    accumulator: LongOption, operand: ByteOption, name: String, record: AnyRef): Unit = {
    checkNull(operand, name, record)
    accumulator.add(operand.get())
  }

  def addWithName(
    accumulator: LongOption, operand: ShortOption, name: String, record: AnyRef): Unit = {
    checkNull(operand, name, record)
    accumulator.add(operand.get())
  }

  def addWithName(
    accumulator: LongOption, operand: IntOption, name: String, record: AnyRef): Unit = {
    checkNull(operand, name, record)
    accumulator.add(operand.get())
  }

  def addWithName(
    accumulator: LongOption, operand: LongOption, name: String, record: AnyRef): Unit = {
    checkNull(operand, name, record)
    accumulator.add(operand)
  }

  def addWithName(
    accumulator: DecimalOption, operand: DecimalOption, name: String, record: AnyRef): Unit = {
    checkNull(operand, name, record)
    accumulator.add(operand)
  }

  def addWithName(
    accumulator: DoubleOption, operand: FloatOption, name: String, record: AnyRef): Unit = {
    checkNull(operand, name, record)
    accumulator.add(operand.get())
  }

  def addWithName(
    accumulator: DoubleOption, operand: DoubleOption, name: String, record: AnyRef): Unit = {
    checkNull(operand, name, record)
    accumulator.add(operand)
  }

  private[this] def checkNull(operand: ValueOption[_], name: String, record: AnyRef): Unit = {
    if (operand.isNull) {
      throw new NullPointerException(
        s"${name} must not be null: ${record}")
    }
  }
}
