package com.asakusafw.spark

import org.apache.spark.SparkConf

package object runtime {

  val AsakusafwConfPrefix = "com.asakusafw.spark"

  object Props {
    val StageInfo = s"${AsakusafwConfPrefix}.stageInfo"

    val Parallelism = s"${AsakusafwConfPrefix}.parallelism"
    val ParallelismScaleSmall = s"${AsakusafwConfPrefix}.parallelism.scale.small"
    val ParallelismScaleLarge = s"${AsakusafwConfPrefix}.parallelism.scala.large"
    val ParallelismScaleHuge = s"${AsakusafwConfPrefix}.parallelism.scala.huge"

    val DefaultParallelismScaleSmall = 0.5
    val DefaultParallelismScaleLarge = 2.0
    val DefaultParallelismScaleHuge = 4.0
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
