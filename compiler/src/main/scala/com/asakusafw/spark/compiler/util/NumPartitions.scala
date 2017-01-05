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
package com.asakusafw.spark.compiler
package util

import org.apache.spark.SparkConf
import org.objectweb.asm.Type

import com.asakusafw.lang.compiler.planning.SubPlan
import com.asakusafw.spark.compiler.planning.PartitionGroupInfo
import com.asakusafw.spark.compiler.util.SparkIdioms._
import com.asakusafw.spark.runtime.Props
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._
import com.asakusafw.spark.tools.asm4s._

object NumPartitions {

  def numPartitions(
    jobContext: => Stack)(
      port: SubPlan.Port)(
        implicit mb: MethodBuilder): Stack = {
    val dataSize = Option(port.getAttribute(classOf[PartitionGroupInfo]))
      .map(_.getDataSize).getOrElse(PartitionGroupInfo.DataSize.REGULAR)
    val scale = getParallelismScale(jobContext) _
    dataSize match {
      case PartitionGroupInfo.DataSize.TINY =>
        ldc(1)
      case PartitionGroupInfo.DataSize.SMALL =>
        invokeStatic(
          classOf[Math].asType,
          "max",
          Type.INT_TYPE,
          getParallelism(jobContext).toDouble.multiply(scale("Small")).toInt,
          ldc(1))
      case PartitionGroupInfo.DataSize.REGULAR =>
        invokeStatic(
          classOf[Math].asType,
          "max",
          Type.INT_TYPE,
          getParallelism(jobContext),
          ldc(1))
      case PartitionGroupInfo.DataSize.LARGE =>
        invokeStatic(
          classOf[Math].asType,
          "max",
          Type.INT_TYPE,
          getParallelism(jobContext).toDouble.multiply(scale("Large")).toInt,
          ldc(1))
      case PartitionGroupInfo.DataSize.HUGE =>
        invokeStatic(
          classOf[Math].asType,
          "max",
          Type.INT_TYPE,
          getParallelism(jobContext).toDouble.multiply(scale("Huge")).toInt,
          ldc(1))
    }
  }

  private def getParallelism(jobContext: => Stack)(implicit mb: MethodBuilder): Stack = {
    sparkContext(jobContext)
      .invokeV("getConf", classOf[SparkConf].asType)
      .invokeV("getInt", Type.INT_TYPE,
        pushObject(Props)
          .invokeV("Parallelism", classOf[String].asType),
        sparkContext(jobContext)
          .invokeV("getConf", classOf[SparkConf].asType)
          .invokeV("getInt", Type.INT_TYPE,
            ldc("spark.default.parallelism"),
            pushObject(Props)
              .invokeV("ParallelismFallback", Type.INT_TYPE)))
  }

  private def getParallelismScale(
    jobContext: => Stack)(
      suffix: String)(
        implicit mb: MethodBuilder): Stack = {
    sparkContext(jobContext)
      .invokeV("getConf", classOf[SparkConf].asType)
      .invokeV("getDouble", Type.DOUBLE_TYPE,
        pushObject(Props)
          .invokeV(s"ParallelismScale${suffix}", classOf[String].asType),
        pushObject(Props)
          .invokeV(s"DefaultParallelismScale${suffix}", Type.DOUBLE_TYPE))
  }
}
