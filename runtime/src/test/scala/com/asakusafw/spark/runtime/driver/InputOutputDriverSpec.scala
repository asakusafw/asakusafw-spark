package com.asakusafw.spark.runtime
package driver

import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner

import java.io.{ DataInput, DataOutput, File }
import java.nio.file.{ Files, Path }

import scala.collection.JavaConversions._
import scala.concurrent.{ Await, Future }
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.reflect.ClassTag

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Writable
import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd._

import com.asakusafw.runtime.model.DataModel
import com.asakusafw.runtime.stage.StageConstants.EXPR_EXECUTION_ID
import com.asakusafw.runtime.value.IntOption
import com.asakusafw.spark.runtime.aggregation.Aggregation
import com.asakusafw.spark.runtime.fragment._
import com.asakusafw.spark.runtime.rdd.BranchKey

@RunWith(classOf[JUnitRunner])
class InputOutputDriverSpecTest extends InputOutputDriverSpec

class InputOutputDriverSpec extends FlatSpec with SparkSugar {

  import InputOutputDriverSpec._

  behavior of "Input/OutputDriver"

  it should "output and input" in {
    val tmpDir = createTempDirectory().toFile
    val path = new File(tmpDir, s"output-${EXPR_EXECUTION_ID}").getAbsolutePath

    val f = new Function1[Int, (_, Hoge)] with Serializable {
      @transient var h: Hoge = _
      def hoge: Hoge = {
        if (h == null) {
          h = new Hoge()
        }
        h
      }
      override def apply(i: Int): (_, Hoge) = {
        hoge.id.modify(i)
        (null, hoge)
      }
    }

    val hoges = sc.parallelize(0 until 10).map(f).asInstanceOf[RDD[(_, Hoge)]]

    new TestOutputDriver(sc, hadoopConf, Future.successful(hoges), path).execute()

    val inputs = new TestInputDriver(sc, hadoopConf, path).execute()
    assert(Await.result(
      inputs(HogeResult).map {
        _.map(_._2.asInstanceOf[Hoge].id.get)
      }, Duration.Inf).collect.toSeq === (0 until 10))
  }

  private def createTempDirectory(): Path = {
    val tmpDir = Files.createTempDirectory(s"test-")
    sys.addShutdownHook {
      def deleteRecursively(path: Path): Unit = {
        if (Files.isDirectory(path)) {
          Files.newDirectoryStream(path).foreach(deleteRecursively)
        }
        Files.delete(path)
      }
      deleteRecursively(tmpDir)
    }
    tmpDir
  }
}

object InputOutputDriverSpec {

  val HogeResult = BranchKey(0)

  class TestOutputDriver(
    @transient sc: SparkContext,
    @transient hadoopConf: Broadcast[Configuration],
    @transient input: Future[RDD[(_, Hoge)]],
    val path: String)
      extends OutputDriver[Hoge](sc, hadoopConf, Seq(input)) {

    override def name = "TestOutput"
  }

  class TestInputDriver(
    @transient sc: SparkContext,
    @transient hadoopConf: Broadcast[Configuration],
    basePath: String)
      extends InputDriver[Hoge](sc, hadoopConf, Map.empty) {

    override def name = "TestInput"

    override def paths: Set[String] = Set(basePath + "/part-*")

    override def branchKeys: Set[BranchKey] = Set(HogeResult)

    override def partitioners: Map[BranchKey, Partitioner] = Map.empty

    override def orderings: Map[BranchKey, Ordering[ShuffleKey]] = Map.empty

    override def aggregations: Map[BranchKey, Aggregation[ShuffleKey, _, _]] = Map.empty

    override def fragments(broadcasts: Map[BroadcastId, Broadcast[_]]): (Fragment[Hoge], Map[BranchKey, OutputFragment[_]]) = {
      val fragment = new HogeOutputFragment
      val outputs = Map(HogeResult -> fragment)
      (fragment, outputs)
    }

    override def shuffleKey(branch: BranchKey, value: Any): ShuffleKey = null
  }

  class Hoge extends DataModel[Hoge] with Writable {

    val id = new IntOption()

    override def reset(): Unit = {
      id.setNull()
    }
    override def copyFrom(other: Hoge): Unit = {
      id.copyFrom(other.id)
    }
    override def readFields(in: DataInput): Unit = {
      id.readFields(in)
    }
    override def write(out: DataOutput): Unit = {
      id.write(out)
    }
  }

  class HogeOutputFragment extends OutputFragment[Hoge] {
    override def newDataModel: Hoge = new Hoge()
  }
}
