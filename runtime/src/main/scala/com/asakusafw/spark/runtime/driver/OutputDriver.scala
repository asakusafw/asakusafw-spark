package com.asakusafw.spark.runtime
package driver

import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
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
import com.asakusafw.spark.runtime.rdd.BranchKey

abstract class OutputDriver[T: ClassTag](
  sc: SparkContext,
  hadoopConf: Broadcast[Configuration],
  @transient prevs: Seq[Future[RDD[(_, T)]]],
  @transient terminators: mutable.Set[Future[Unit]])
    extends SubPlanDriver(sc, hadoopConf, Map.empty) {
  assert(prevs.size > 0,
    s"Previous RDDs should be more than 0: ${prevs.size}")

  val Logger = LoggerFactory.getLogger(getClass())

  override def execute(): Map[BranchKey, Future[RDD[(ShuffleKey, _)]]] = {
    val job = JobCompatibility.newJob(sc.hadoopConfiguration)
    job.setOutputKeyClass(classOf[NullWritable])
    job.setOutputValueClass(classTag[T].runtimeClass.asInstanceOf[Class[T]])
    job.setOutputFormatClass(classOf[TemporaryOutputFormat[T]])

    val conf = sc.getConf
    val stageInfo = StageInfo.deserialize(conf.getHadoopConf(Props.StageInfo))
    TemporaryOutputFormat.setOutputPath(job, new Path(stageInfo.resolveVariables(path)))

    val future = (if (prevs.size == 1) {
      prevs.head
    } else {
      ((prevs :\ Future.successful(List.empty[RDD[(_, T)]])) {
        case (prev, list) => prev.zip(list).map {
          case (p, l) => p :: l
        }
      }).map(new UnionRDD(sc, _).coalesce(sc.defaultParallelism, shuffle = false))
    })
      .map { prev =>

        sc.clearCallSite()
        sc.setCallSite(name)

        val output = prev.map(in => (NullWritable.get, in._2))

        if (Logger.isDebugEnabled()) {
          Logger.debug(output.toDebugString)
        }

        output.saveAsNewAPIHadoopDataset(job.getConfiguration)
      }

    terminators += future

    Map.empty
  }

  def path: String
}
