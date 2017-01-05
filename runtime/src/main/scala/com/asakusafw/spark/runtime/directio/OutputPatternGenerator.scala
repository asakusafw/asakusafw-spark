/*
 * Copyright 2011-2017 Asakusa Framework Team.
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

import java.text.SimpleDateFormat
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

  def date(property: String, formatString: String): Fragment =
    Fragment.DateFormat(property, formatString)

  def dateTime(property: String, formatString: String): Fragment =
    Fragment.DateTimeFormat(property, formatString)

  def random(seed: Long, min: Int, max: Int): Fragment =
    Fragment.RandomNumber(seed, min, max)

  sealed trait Fragment

  object Fragment {

    case class Constant(value: String) extends Fragment

    case class Natural(property: String) extends Fragment

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
