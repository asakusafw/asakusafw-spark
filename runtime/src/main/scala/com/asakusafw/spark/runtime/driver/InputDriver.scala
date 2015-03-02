package com.asakusafw.spark.runtime.driver

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.reflect.{ classTag, ClassTag }

import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.Job
import org.apache.spark._
import org.apache.spark.rdd.RDD

import com.asakusafw.runtime.model.DataModel
import com.asakusafw.runtime.stage.input.TemporaryInputFormat
import com.asakusafw.spark.runtime.fragment._
import com.asakusafw.spark.runtime.rdd._

abstract class InputDriver[T <: DataModel[T]: ClassTag, B](
  @transient sc: SparkContext)
    extends SubPlanDriver[B] with Branch[B] {

  override def execute(): Map[B, RDD[(_, _)]] = {
    val job = Job.getInstance(sc.hadoopConfiguration)
    TemporaryInputFormat.setInputPaths(job, paths.map(new Path(_)).toSeq)
    sc.newAPIHadoopRDD(
      job.getConfiguration,
      classOf[TemporaryInputFormat[T]],
      classOf[NullWritable],
      classTag[T].runtimeClass.asInstanceOf[Class[T]])
      .branch[B, Any, Any](branchKeys, { iter =>
        val (fragment, outputs) = fragments
        assert(outputs.keys.toSet == branchKeys)
        iter.flatMap {
          case (_, dm) =>
            fragment.reset()
            fragment.add(dm)
            outputs.iterator.flatMap {
              case (key, output) =>
                def prepare[T <: DataModel[T]](buffer: mutable.ArrayBuffer[_]) = {
                  buffer.asInstanceOf[mutable.ArrayBuffer[T]]
                    .map(out => ((key, shuffleKey(key, out)), out))
                }
                prepare(output.buffer)
            }
        }
      },
        partitioners = partitioners,
        keyOrderings = orderings,
        preservesPartitioning = true)
  }

  def paths: Set[String]

  def fragments[U <: DataModel[U]]: (Fragment[T], Map[B, OutputFragment[U]])
}
