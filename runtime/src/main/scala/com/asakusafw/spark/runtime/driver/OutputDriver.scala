package com.asakusafw.spark.runtime
package driver

import scala.collection.mutable
import scala.concurrent.Future
import scala.reflect.{ classTag, ClassTag }

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.fs.Path
import org.apache.spark.{ Partitioner, SparkContext }
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.{ RDD, UnionRDD }
import org.slf4j.LoggerFactory

import com.asakusafw.bridge.stage.StageInfo
import com.asakusafw.runtime.compatibility.JobCompatibility
import com.asakusafw.runtime.stage.output.TemporaryOutputFormat
import com.asakusafw.spark.runtime.SparkClient.executionContext
import com.asakusafw.spark.runtime.rdd._

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

    val stageInfo = StageInfo.deserialize(job.getConfiguration.get(StageInfo.KEY_NAME))
    TemporaryOutputFormat.setOutputPath(job, new Path(stageInfo.resolveVariables(path)))

    val future = (if (prevs.size == 1) {
      prevs.head
    } else {
      Future.sequence(prevs).map { prevs =>
        val part = Partitioner.defaultPartitioner(prevs.head, prevs.tail: _*)
        val (unioning, coalescing) = prevs.partition(_.partitions.size < part.numPartitions)
        val coalesced = zipPartitions(
          coalescing.map {
            case prev if prev.partitions.size == part.numPartitions => prev
            case prev => prev.coalesce(part.numPartitions, shuffle = false)
          }, preservesPartitioning = false) {
            _.iterator.flatten.asInstanceOf[Iterator[(_, T)]]
          }
        if (unioning.isEmpty) {
          coalesced
        } else {
          new UnionRDD(sc, coalesced +: unioning)
        }
      }
    })
      .map { prev =>

        sc.clearCallSite()
        sc.setCallSite(label)

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
