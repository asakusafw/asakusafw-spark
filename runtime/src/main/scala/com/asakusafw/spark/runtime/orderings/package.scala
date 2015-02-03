package com.asakusafw.spark.runtime

import com.asakusafw.runtime.value._

package object orderings {

  trait BooleanOptionOrdering extends Ordering[BooleanOption] {
    def compare(x: BooleanOption, y: BooleanOption): Int = x.compareTo(y)
  }
  implicit object BooleanOption extends BooleanOptionOrdering

  trait ByteOptionOrdering extends Ordering[ByteOption] {
    def compare(x: ByteOption, y: ByteOption): Int = x.compareTo(y)
  }
  implicit object ByteOption extends ByteOptionOrdering

  trait ShortOptionOrdering extends Ordering[ShortOption] {
    def compare(x: ShortOption, y: ShortOption): Int = x.compareTo(y)
  }
  implicit object ShortOption extends ShortOptionOrdering

  trait IntOptionOrdering extends Ordering[IntOption] {
    def compare(x: IntOption, y: IntOption): Int = x.compareTo(y)
  }
  implicit object IntOption extends IntOptionOrdering

  trait LongOptionOrdering extends Ordering[LongOption] {
    def compare(x: LongOption, y: LongOption): Int = x.compareTo(y)
  }
  implicit object LongOption extends LongOptionOrdering

  trait FloatOptionOrdering extends Ordering[FloatOption] {
    def compare(x: FloatOption, y: FloatOption): Int = x.compareTo(y)
  }
  implicit object FloatOption extends FloatOptionOrdering

  trait DoubleOptionOrdering extends Ordering[DoubleOption] {
    def compare(x: DoubleOption, y: DoubleOption): Int = x.compareTo(y)
  }
  implicit object DoubleOption extends DoubleOptionOrdering

  trait DecimalOptionOrdering extends Ordering[DecimalOption] {
    def compare(x: DecimalOption, y: DecimalOption): Int = x.compareTo(y)
  }
  implicit object DecimalOption extends DecimalOptionOrdering

  trait StringOptionOrdering extends Ordering[StringOption] {
    def compare(x: StringOption, y: StringOption): Int = x.compareTo(y)
  }
  implicit object StringOption extends StringOptionOrdering

  trait DateOptionOrdering extends Ordering[DateOption] {
    def compare(x: DateOption, y: DateOption): Int = x.compareTo(y)
  }
  implicit object DateOption extends DateOptionOrdering

  trait DateTimeOptionOrdering extends Ordering[DateTimeOption] {
    def compare(x: DateTimeOption, y: DateTimeOption): Int = x.compareTo(y)
  }
  implicit object DateTimeOption extends DateTimeOptionOrdering
}
