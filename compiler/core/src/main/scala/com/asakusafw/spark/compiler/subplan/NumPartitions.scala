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
        getParallelism(mb, sc).toDouble.multiply(getParallelismScale(mb, sc)("Small")).toInt
      case PartitionGroupInfo.DataSize.REGULAR =>
        getParallelism(mb, sc)
      case PartitionGroupInfo.DataSize.LARGE =>
        getParallelism(mb, sc).toDouble.multiply(getParallelismScale(mb, sc)("Large")).toInt
      case PartitionGroupInfo.DataSize.HUGE =>
        getParallelism(mb, sc).toDouble.multiply(getParallelismScale(mb, sc)("Huge")).toInt
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
