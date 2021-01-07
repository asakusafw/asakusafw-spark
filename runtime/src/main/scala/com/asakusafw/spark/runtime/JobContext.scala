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
package com.asakusafw.spark.runtime

import scala.collection.JavaConversions._
import scala.collection.mutable

import org.apache.spark.SparkContext
import org.apache.spark.util._

import com.asakusafw.spark.runtime.JobContext._

trait JobContext {

  def sparkContext: SparkContext

  @transient
  val inputStatistics: mutable.Map[InputCounter, mutable.Map[String, InputStatistics]] =
    mutable.Map.empty

  def getOrNewInputStatistics(
    counter: InputCounter, name: String): InputStatistics = {
    inputStatistics
      .getOrElseUpdate(counter, mutable.Map.empty)
      .getOrElseUpdate(
        name,
        new InputStatistics(
          sparkContext.longAccumulator(s"input.${counter.name}.${name}.records")))
  }

  @transient
  val outputStatistics: mutable.Map[OutputCounter, mutable.Map[String, OutputStatistics]] =
    mutable.Map.empty

  def getOrNewOutputStatistics(
    counter: OutputCounter, name: String): OutputStatistics = {
    outputStatistics
      .getOrElseUpdate(counter, mutable.Map.empty)
      .getOrElseUpdate(
        name,
        new OutputStatistics(
          sparkContext.collectionAccumulator(s"output.${counter.name}.${name}.files"),
          sparkContext.longAccumulator(s"output.${counter.name}.${name}.bytes"),
          sparkContext.longAccumulator(s"output.${counter.name}.${name}.records")))
  }
}

object JobContext {

  sealed abstract class InputCounter(val name: String)

  object InputCounter {
    case object Direct extends InputCounter("direct")
    case object External extends InputCounter("external")
  }

  class InputStatistics private[JobContext] (
    recordCounter: LongAccumulator) extends Serializable {

    def addRecords(records: Long): Unit = recordCounter.add(records)

    def records: Long = recordCounter.value

    override def toString(): String =
      s"InputStatistics(records=${records})"
  }

  sealed abstract class OutputCounter(val name: String)

  object OutputCounter {
    case object Direct extends OutputCounter("direct")
    case object External extends OutputCounter("external")
  }

  class OutputStatistics private[JobContext] (
    fileAccumulator: CollectionAccumulator[String],
    byteCounter: LongAccumulator,
    recordCounter: LongAccumulator) extends Serializable {

    def addFile(name: String): Unit = fileAccumulator.add(name)

    def files: Int = fileAccumulator.value.toSet.size

    def addBytes(bytes: Long): Unit = byteCounter.add(bytes)

    def bytes: Long = byteCounter.value

    def addRecords(records: Long): Unit = recordCounter.add(records)

    def records: Long = recordCounter.value

    override def toString(): String =
      s"OutputStatistics(files=${files},bytes=${bytes},records=${records})"
  }
}
