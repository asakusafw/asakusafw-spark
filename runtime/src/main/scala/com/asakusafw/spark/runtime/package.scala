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
package com.asakusafw.spark

import org.apache.spark.SparkConf

package object runtime {

  val AsakusafwConfPrefix = "com.asakusafw.spark"

  object Props {
    val Parallelism = s"${AsakusafwConfPrefix}.parallelism"
    val ParallelismScaleSmall = s"${AsakusafwConfPrefix}.parallelism.scale.small"
    val ParallelismScaleLarge = s"${AsakusafwConfPrefix}.parallelism.scala.large"
    val ParallelismScaleHuge = s"${AsakusafwConfPrefix}.parallelism.scala.huge"

    val ParallelismFallback = 2
    val DefaultParallelismScaleSmall = 0.5
    val DefaultParallelismScaleLarge = 2.0
    val DefaultParallelismScaleHuge = 4.0

    val FragmentBufferSize = s"${AsakusafwConfPrefix}.fragment.bufferSize"

    val DefaultFragmentBufferSize = -1
  }

  val HadoopConfPrefix = "spark.hadoop"

  implicit class AugmentedSparkConf(val conf: SparkConf) extends AnyVal {

    def setHadoopConf(key: String, value: String): SparkConf =
      conf.set(s"${HadoopConfPrefix}.${key}", value)

    def getHadoopConf(key: String): String =
      conf.get(s"${HadoopConfPrefix}.${key}")
    def getHadoopConf(key: String, defaultValue: String): String =
      conf.get(s"${HadoopConfPrefix}.${key}", defaultValue)

    def removeHadoopConf(key: String): SparkConf =
      conf.remove(s"${HadoopConfPrefix}.${key}")

    def getHadoopConfOption(key: String): Option[String] = {
      conf.getOption(s"${HadoopConfPrefix}.${key}")
    }

    def getAllHadoopConf: Array[(String, String)] = {
      conf.getAll.filter(_._1.startsWith(s"${HadoopConfPrefix}."))
    }
  }
}
