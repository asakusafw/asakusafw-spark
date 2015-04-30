package com.asakusafw.spark.runtime
package driver

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.concurrent.Future
import scala.reflect.{ classTag, ClassTag }

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.Job
import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import org.apache.spark.backdoor._
import org.apache.spark.util.backdoor._
import com.asakusafw.bridge.stage.StageInfo
import com.asakusafw.runtime.compatibility.JobCompatibility
import com.asakusafw.runtime.stage.input.TemporaryInputFormat
import com.asakusafw.spark.runtime.rdd.BranchKey

abstract class InputDriver[T: ClassTag](
  sc: SparkContext,
  hadoopConf: Broadcast[Configuration],
  broadcasts: Map[BroadcastId, Future[Broadcast[_]]])
    extends SubPlanDriver(sc, hadoopConf, broadcasts) with Branching[T] {

  def paths: Set[String]

  override def execute(): Map[BranchKey, RDD[(ShuffleKey, _)]] = {
    val job = JobCompatibility.newJob(sc.hadoopConfiguration)

    val conf = sc.getConf
    val stageInfo = StageInfo.deserialize(conf.getHadoopConf(Props.StageInfo))
    TemporaryInputFormat.setInputPaths(job, paths.map { path =>
      new Path(stageInfo.resolveVariables(path))
    }.toSeq)

    sc.clearCallSite()
    sc.setCallSite(name)

    val rdd =
      sc.newAPIHadoopRDD(
        job.getConfiguration,
        classOf[TemporaryInputFormat[T]],
        classOf[NullWritable],
        classTag[T].runtimeClass.asInstanceOf[Class[T]])

    //    sc.setCallSite(CallSite(name, rdd.toDebugString))
    branch(rdd.asInstanceOf[RDD[(_, T)]])
  }
}
