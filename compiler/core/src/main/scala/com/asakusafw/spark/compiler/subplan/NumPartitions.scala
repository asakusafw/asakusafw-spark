/*
 * Copyright 2011-2015 Asakusa Framework Team.
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
package com.asakusafw.spark.compiler.subplan

import org.apache.spark.SparkConf
import org.objectweb.asm.Type

import com.asakusafw.lang.compiler.planning.SubPlan
import com.asakusafw.spark.compiler.planning.PartitionGroupInfo
import com.asakusafw.spark.runtime.Props
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._

trait NumPartitions {

  def numPartitions(mb: MethodBuilder, sc: => Stack)(port: SubPlan.Port): Stack = {
    import mb._
    val dataSize = Option(port.getAttribute(classOf[PartitionGroupInfo]))
      .map(_.getDataSize).getOrElse(PartitionGroupInfo.DataSize.REGULAR)
    dataSize match {
      case PartitionGroupInfo.DataSize.TINY =>
        ldc(1)
      case PartitionGroupInfo.DataSize.SMALL =>
        invokeStatic(
          classOf[Math].asType,
          "max",
          Type.INT_TYPE,
          getParallelism(mb, sc).toDouble.multiply(getParallelismScale(mb, sc)("Small")).toInt,
          ldc(1))
      case PartitionGroupInfo.DataSize.REGULAR =>
        invokeStatic(
          classOf[Math].asType,
          "max",
          Type.INT_TYPE,
          getParallelism(mb, sc),
          ldc(1))
      case PartitionGroupInfo.DataSize.LARGE =>
        invokeStatic(
          classOf[Math].asType,
          "max",
          Type.INT_TYPE,
          getParallelism(mb, sc).toDouble.multiply(getParallelismScale(mb, sc)("Large")).toInt,
          ldc(1))
      case PartitionGroupInfo.DataSize.HUGE =>
        invokeStatic(
          classOf[Math].asType,
          "max",
          Type.INT_TYPE,
          getParallelism(mb, sc).toDouble.multiply(getParallelismScale(mb, sc)("Huge")).toInt,
          ldc(1))
    }
  }

  private def getParallelism(mb: MethodBuilder, sc: => Stack): Stack = {
    import mb._
    sc.invokeV("getConf", classOf[SparkConf].asType)
      .invokeV("getInt", Type.INT_TYPE,
        getStatic(Props.getClass.asType, "MODULE$", Props.getClass.asType)
          .invokeV("Parallelism", classOf[String].asType),
        sc.invokeV("getConf", classOf[SparkConf].asType)
          .invokeV("getInt", Type.INT_TYPE,
            ldc("spark.default.parallelism"),
            getStatic(Props.getClass.asType, "MODULE$", Props.getClass.asType)
              .invokeV("ParallelismFallback", Type.INT_TYPE)))
  }

  private def getParallelismScale(mb: MethodBuilder, sc: => Stack)(suffix: String): Stack = {
    import mb._
    sc.invokeV("getConf", classOf[SparkConf].asType)
      .invokeV("getDouble", Type.DOUBLE_TYPE,
        getStatic(Props.getClass.asType, "MODULE$", Props.getClass.asType)
          .invokeV(s"ParallelismScale${suffix}", classOf[String].asType),
        getStatic(Props.getClass.asType, "MODULE$", Props.getClass.asType)
          .invokeV(s"DefaultParallelismScale${suffix}", Type.DOUBLE_TYPE))
  }
}
