/*
 * Copyright 2011-2018 Asakusa Framework Team.
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
package com.asakusafw.spark.runtime.directio

import java.text.{ DecimalFormat => JDecimalFormat, SimpleDateFormat }
import java.util.{ Calendar, Random }

import com.asakusafw.bridge.stage.StageInfo
import com.asakusafw.runtime.value._
import com.asakusafw.spark.runtime.directio.OutputPatternGenerator._

abstract class OutputPatternGenerator[T](fragments: Seq[Fragment]) {

  private val strOpt = new StringOption()

  def getProperty(target: T, name: String): ValueOption[_]

  def generate(target: T)(stageInfo: StageInfo): StringOption = {
    val str = fragments.map {
      case Fragment.Constant(value) => stageInfo.resolveUserVariables(value)
      case Fragment.Natural(property) => getProperty(target, property)
      case num: Fragment.NumberFormat[_] =>
        num.format(getProperty(target, num.property))
      case date: Fragment.DateFormat =>
        date.format(getProperty(target, date.property))
      case dateTime: Fragment.DateTimeFormat =>
        dateTime.format(getProperty(target, dateTime.property))
      case rnd: Fragment.RandomNumber => rnd.nextInt()
    }.mkString
    strOpt.modify(str)
    strOpt
  }
}

object OutputPatternGenerator {

  def constant(value: String): Fragment = Fragment.Constant(value)

  def natural(property: String): Fragment = Fragment.Natural(property)

  def byte(property: String, formatString: String): Fragment =
    Fragment.ByteFormat(property, formatString)

  def short(property: String, formatString: String): Fragment =
    Fragment.ShortFormat(property, formatString)

  def int(property: String, formatString: String): Fragment =
    Fragment.IntFormat(property, formatString)

  def long(property: String, formatString: String): Fragment =
    Fragment.LongFormat(property, formatString)

  def float(property: String, formatString: String): Fragment =
    Fragment.FloatFormat(property, formatString)

  def double(property: String, formatString: String): Fragment =
    Fragment.DoubleFormat(property, formatString)

  def decimal(property: String, formatString: String): Fragment =
    Fragment.DecimalFormat(property, formatString)

  def date(property: String, formatString: String): Fragment =
    Fragment.DateFormat(property, formatString)

  def datetime(property: String, formatString: String): Fragment =
    Fragment.DateTimeFormat(property, formatString)

  def random(seed: Long, min: Int, max: Int): Fragment =
    Fragment.RandomNumber(seed, min, max)

  sealed trait Fragment

  object Fragment {

    case class Constant(value: String) extends Fragment

    case class Natural(property: String) extends Fragment

    sealed abstract class NumberFormat[T <: ValueOption[T]] extends Fragment {

      def property: String

      def formatString: String

      protected val nf = new JDecimalFormat(formatString)

      def format(propertyValue: ValueOption[_]): String = {
        if (propertyValue.isNull()) {
          String.valueOf(propertyValue)
        } else {
          formatValue(propertyValue.asInstanceOf[T])
        }
      }

      def formatValue(opt: T): String
    }

    case class ByteFormat(property: String, formatString: String)
      extends NumberFormat[ByteOption] {

      override def formatValue(opt: ByteOption): String = nf.format(opt.get)
    }

    case class ShortFormat(property: String, formatString: String)
      extends NumberFormat[ShortOption] {

      override def formatValue(opt: ShortOption): String = nf.format(opt.get)
    }

    case class IntFormat(property: String, formatString: String)
      extends NumberFormat[IntOption] {

      override def formatValue(opt: IntOption): String = nf.format(opt.get)
    }

    case class LongFormat(property: String, formatString: String)
      extends NumberFormat[LongOption] {

      override def formatValue(opt: LongOption): String = nf.format(opt.get)
    }

    case class FloatFormat(property: String, formatString: String)
      extends NumberFormat[FloatOption] {

      override def formatValue(opt: FloatOption): String = nf.format(opt.get)
    }

    case class DoubleFormat(property: String, formatString: String)
      extends NumberFormat[DoubleOption] {

      override def formatValue(opt: DoubleOption): String = nf.format(opt.get)
    }

    case class DecimalFormat(property: String, formatString: String)
      extends NumberFormat[DecimalOption] {

      override def formatValue(opt: DecimalOption): String = nf.format(opt.get)
    }

    case class DateFormat(property: String, formatString: String) extends Fragment {

      private val calendar = Calendar.getInstance()
      private val sdf = new SimpleDateFormat(formatString)

      def format(propertyValue: ValueOption[_]): String = {
        format(propertyValue.asInstanceOf[DateOption])
      }

      def format(dateOpt: DateOption): String = {
        if (dateOpt.isNull()) {
          String.valueOf(dateOpt)
        } else {
          val date = dateOpt.get
          DateUtil.setDayToCalendar(date.getElapsedDays, calendar)
          String.valueOf(sdf.format(calendar.getTime))
        }
      }
    }

    case class DateTimeFormat(property: String, formatString: String) extends Fragment {

      private val calendar = Calendar.getInstance()
      private val sdf = new SimpleDateFormat(formatString)

      def format(propertyValue: ValueOption[_]): String = {
        format(propertyValue.asInstanceOf[DateTimeOption])
      }

      def format(dateTimeOpt: DateTimeOption): String = {
        if (dateTimeOpt.isNull()) {
          String.valueOf(dateTimeOpt)
        } else {
          val dateTime = dateTimeOpt.get
          DateUtil.setSecondToCalendar(dateTime.getElapsedSeconds, calendar)
          String.valueOf(sdf.format(calendar.getTime))
        }
      }
    }

    case class RandomNumber(seed: Long, min: Int, max: Int) extends Fragment {
      require(min <= max)
      private val rnd = new Random(seed)
      def nextInt(): Int = rnd.nextInt(max - min + 1) + min
    }
  }
}
