package com.asakusafw.spark.runtime.driver

import scala.reflect.{ classTag, ClassTag }

import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.Job
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import com.asakusafw.runtime.model.DataModel
import com.asakusafw.runtime.stage.output.TemporaryOutputFormat

abstract class OutputDriver[T <: DataModel[T]: ClassTag](
  @transient val sc: SparkContext,
  @transient input: RDD[T])
    extends SubPlanDriver[Nothing] {

  override def execute(): Map[Nothing, RDD[(_, _)]] = {
    val job = Job.getInstance(sc.hadoopConfiguration)
    job.setOutputKeyClass(classOf[NullWritable])
    job.setOutputValueClass(classTag[T].runtimeClass.asInstanceOf[Class[T]])
    job.setOutputFormatClass(classOf[TemporaryOutputFormat[T]])
    TemporaryOutputFormat.setOutputPath(job, new Path(path))
    input.map((NullWritable.get, _)).saveAsNewAPIHadoopDataset(job.getConfiguration)
    Map.empty
  }

  def path: String
}
