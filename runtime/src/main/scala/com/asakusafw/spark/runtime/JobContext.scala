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
package com.asakusafw.spark.runtime

import scala.collection.JavaConversions._
import scala.collection.mutable

import org.apache.spark.SparkContext
import org.apache.spark.util._

trait JobContext {

  def sparkContext: SparkContext

  val outputStatistics: mutable.Map[String, JobContext.OutputStatistics] = mutable.Map.empty

  def getOrNewOutputStatistics(name: String): JobContext.OutputStatistics = {
    outputStatistics.getOrElseUpdate(
      name,
      new JobContext.OutputStatistics(
        sparkContext.collectionAccumulator(s"output.${name}.files"),
        sparkContext.longAccumulator(s"output.${name}.bytes"),
        sparkContext.longAccumulator(s"output.${name}.records")))
  }
}

object JobContext {

  class OutputStatistics(
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
