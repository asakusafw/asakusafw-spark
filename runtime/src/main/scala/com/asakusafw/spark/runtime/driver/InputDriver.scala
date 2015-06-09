package com.asakusafw.spark.runtime
package driver

import scala.concurrent.Future
import scala.reflect.{ classTag, ClassTag }

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.InputFormat
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import com.asakusafw.bridge.stage.StageInfo
import com.asakusafw.runtime.compatibility.JobCompatibility
import com.asakusafw.spark.runtime.SparkClient.executionContext
import com.asakusafw.spark.runtime.rdd.BranchKey

abstract class InputDriver[K: ClassTag, V: ClassTag, IF <: InputFormat[K, V]: ClassTag](
  sc: SparkContext,
  hadoopConf: Broadcast[Configuration],
  broadcasts: Map[BroadcastId, Future[Broadcast[_]]])
    extends SubPlanDriver(sc, hadoopConf, broadcasts) with Branching[V] {

  def paths: Option[Set[String]]

  def extraConfigurations: Map[String, String]

  override def execute(): Map[BranchKey, Future[RDD[(ShuffleKey, _)]]] = {
    val job = JobCompatibility.newJob(sc.hadoopConfiguration)

    paths.foreach { ps =>
      val stageInfo = StageInfo.deserialize(job.getConfiguration.get(StageInfo.KEY_NAME))
      FileInputFormat.setInputPaths(job, ps.map { path =>
        new Path(stageInfo.resolveVariables(path))
      }.toSeq: _*)
    }

    extraConfigurations.foreach {
      case (k, v) => job.getConfiguration.set(k, v)
    }

    val future = Future {
      sc.clearCallSite()
      sc.setCallSite(label)

      val rdd =
        sc.newAPIHadoopRDD(
          job.getConfiguration,
          classTag[IF].runtimeClass.asInstanceOf[Class[IF]],
          classTag[K].runtimeClass.asInstanceOf[Class[K]],
          classTag[V].runtimeClass.asInstanceOf[Class[V]])

      branch(rdd.asInstanceOf[RDD[(_, V)]])
    }
    branchKeys.map(key => key -> future.map(_(key))).toMap
  }
}
