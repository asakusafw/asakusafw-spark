package com.asakusafw.spark.runtime
package driver

import scala.reflect.{ classTag, ClassTag }

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.Job
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd._
import org.slf4j.LoggerFactory

import org.apache.spark.backdoor._
import org.apache.spark.util.backdoor.CallSite
import com.asakusafw.bridge.stage.StageInfo
import com.asakusafw.runtime.compatibility.JobCompatibility
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.runtime.stage.output.TemporaryOutputFormat
import com.asakusafw.runtime.util.VariableTable

abstract class OutputDriver[T: ClassTag, B](
  sc: SparkContext,
  hadoopConf: Broadcast[Configuration],
  @transient prevs: Seq[RDD[(_, T)]])
    extends SubPlanDriver[B](sc, hadoopConf, Map.empty) {
  assert(prevs.size > 0,
    s"Previous RDDs should be more than 0: ${prevs.size}")

  val Logger = LoggerFactory.getLogger(getClass())

  override def execute(): Map[BranchKey, RDD[(ShuffleKey, _)]] = {
    val job = JobCompatibility.newJob(sc.hadoopConfiguration)
    job.setOutputKeyClass(classOf[NullWritable])
    job.setOutputValueClass(classTag[T].runtimeClass.asInstanceOf[Class[T]])
    job.setOutputFormatClass(classOf[TemporaryOutputFormat[T]])

    val conf = sc.getConf
    val stageInfo = StageInfo.deserialize(conf.getHadoopConf(Props.StageInfo))
    TemporaryOutputFormat.setOutputPath(job, new Path(stageInfo.resolveVariables(path)))

    sc.clearCallSite()
    sc.setCallSite(name)

    val output = (if (prevs.size == 1) prevs.head else new UnionRDD(sc, prevs))
      .map(in => (NullWritable.get, in._2))

    if (Logger.isDebugEnabled()) {
      Logger.debug(output.toDebugString)
    }

    sc.setCallSite(CallSite(name, output.toDebugString))
    output.saveAsNewAPIHadoopDataset(job.getConfiguration)
    Map.empty
  }

  def path: String
}
