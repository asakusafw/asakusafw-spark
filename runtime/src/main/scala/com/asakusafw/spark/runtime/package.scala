package com.asakusafw.spark

import org.apache.spark.SparkConf

package object runtime {

  val HadoopConfPrefix = "spark.hadoop."

  implicit class AugmentedSparkConf(val conf: SparkConf) extends AnyVal {

    def setHadoopConf(key: String, value: String): SparkConf =
      conf.set(HadoopConfPrefix + key, value)

    def getHadoopConf(key: String): String =
      conf.get(HadoopConfPrefix + key)
    def getHadoopConf(key: String, defaultValue: String): String =
      conf.get(HadoopConfPrefix + key, defaultValue)

    def removeHadoopConf(key: String): SparkConf =
      conf.remove(HadoopConfPrefix + key)

    def getHadoopConfOption(key: String): Option[String] = {
      conf.getOption(HadoopConfPrefix + key)
    }

    def getAllHadoopConf: Array[(String, String)] = {
      conf.getAll.filter(_._1.startsWith(HadoopConfPrefix))
    }
  }
}
