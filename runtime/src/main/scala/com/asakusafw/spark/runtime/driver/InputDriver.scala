package com.asakusafw.spark.runtime
package driver

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.reflect.{ classTag, ClassTag }

import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.Job
import org.apache.spark._
import org.apache.spark.rdd.RDD

import com.asakusafw.runtime.compatibility.JobCompatibility
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.runtime.stage.StageConstants._
import com.asakusafw.runtime.stage.input.TemporaryInputFormat
import com.asakusafw.runtime.util.VariableTable
import com.asakusafw.spark.runtime.fragment._
import com.asakusafw.spark.runtime.rdd._

abstract class InputDriver[T <: DataModel[T]: ClassTag, B](
  @transient val sc: SparkContext)
    extends SubPlanDriver[B] with PrepareKey[B] {

  def paths: Set[String]

  def branchKey: B

  override def execute(): Map[B, RDD[(_, _)]] = {
    val job = JobCompatibility.newJob(sc.hadoopConfiguration)

    val conf = sc.getConf
    TemporaryInputFormat.setInputPaths(job, paths.map { path =>
      val table = new VariableTable(VariableTable.RedefineStrategy.ERROR)
      for (prop <- Seq(PROP_BATCH_ID, PROP_FLOW_ID, PROP_EXECUTION_ID, PROP_ASAKUSA_BATCH_ARGS)) {
        table.defineVariable(prop, conf.getHadoopConf(prop))
      }
      new Path(table.parse(path, true))
    }.toSeq)

    val rdd = sc.newAPIHadoopRDD(
      job.getConfiguration,
      classOf[TemporaryInputFormat[T]],
      classOf[NullWritable],
      classTag[T].runtimeClass.asInstanceOf[Class[T]])
    Map(branchKey -> rdd.map {
      case (_, dm) =>
        (shuffleKey(branchKey, dm), dm)
    })
  }
}
