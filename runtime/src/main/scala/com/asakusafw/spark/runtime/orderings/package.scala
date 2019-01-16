/*
 * Copyright 2011-2019 Asakusa Framework Team.
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
package com.asakusafw.spark.runtime

import com.asakusafw.runtime.value._

package object orderings {

  trait BooleanOptionOrdering extends Ordering[BooleanOption] {
    def compare(x: BooleanOption, y: BooleanOption): Int = x.compareTo(y)
  }
  implicit object BooleanOptionOrdering extends BooleanOptionOrdering

  trait ByteOptionOrdering extends Ordering[ByteOption] {
    def compare(x: ByteOption, y: ByteOption): Int = x.compareTo(y)
  }
  implicit object ByteOptionOrdering extends ByteOptionOrdering

  trait ShortOptionOrdering extends Ordering[ShortOption] {
    def compare(x: ShortOption, y: ShortOption): Int = x.compareTo(y)
  }
  implicit object ShortOptionOrdering extends ShortOptionOrdering

  trait IntOptionOrdering extends Ordering[IntOption] {
    def compare(x: IntOption, y: IntOption): Int = x.compareTo(y)
  }
  implicit object IntOptionOrdering extends IntOptionOrdering

  trait LongOptionOrdering extends Ordering[LongOption] {
    def compare(x: LongOption, y: LongOption): Int = x.compareTo(y)
  }
  implicit object LongOptionOrdering extends LongOptionOrdering

  trait FloatOptionOrdering extends Ordering[FloatOption] {
    def compare(x: FloatOption, y: FloatOption): Int = x.compareTo(y)
  }
  implicit object FloatOptionOrdering extends FloatOptionOrdering

  trait DoubleOptionOrdering extends Ordering[DoubleOption] {
    def compare(x: DoubleOption, y: DoubleOption): Int = x.compareTo(y)
  }
  implicit object DoubleOptionOrdering extends DoubleOptionOrdering

  trait DecimalOptionOrdering extends Ordering[DecimalOption] {
    def compare(x: DecimalOption, y: DecimalOption): Int = x.compareTo(y)
  }
  implicit object DecimalOptionOrdering extends DecimalOptionOrdering

  trait StringOptionOrdering extends Ordering[StringOption] {
    def compare(x: StringOption, y: StringOption): Int = x.compareTo(y)
  }
  implicit object StringOptionOrdering extends StringOptionOrdering

  trait DateOptionOrdering extends Ordering[DateOption] {
    def compare(x: DateOption, y: DateOption): Int = x.compareTo(y)
  }
  implicit object DateOptionOrdering extends DateOptionOrdering

  trait DateTimeOptionOrdering extends Ordering[DateTimeOption] {
    def compare(x: DateTimeOption, y: DateTimeOption): Int = x.compareTo(y)
  }
  implicit object DateTimeOptionOrdering extends DateTimeOptionOrdering
}
