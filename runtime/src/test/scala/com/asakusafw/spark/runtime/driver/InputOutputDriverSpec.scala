package com.asakusafw.spark.runtime
package driver

import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner

import java.io.{ DataInput, DataOutput, File }

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

@RunWith(classOf[JUnitRunner])
class InputOutputDriverSpecTest extends InputOutputDriverSpec

class InputOutputDriverSpec extends FlatSpec with SparkSugar {

  import InputOutputDriverSpec._

  behavior of "Input/OutputDriver"

  it should "output and input" in {
    val tmpDir = File.createTempFile(s"test-${EXPR_EXECUTION_ID}-", null)
    tmpDir.delete
    val path = tmpDir.getAbsolutePath

    val hoges = sc.parallelize(0 until 10).map { i =>
      val hoge = new Hoge()
      hoge.id.modify(i)
      (hoge, hoge)
    }

    new TestOutputDriver(sc, hadoopConf, hoges.asInstanceOf[RDD[(_, Hoge)]], path).execute()

    val inputs = new TestInputDriver(sc, hadoopConf, path).execute()
    assert(inputs(HogeResult).map(_._2.asInstanceOf[Hoge].id.get).collect.toSeq === (0 until 10))
  }
}

object InputOutputDriverSpec {

  val HogeResult = BranchKey(0)

  class TestOutputDriver(
    @transient sc: SparkContext,
    @transient hadoopConf: Broadcast[Configuration],
    @transient input: RDD[(_, Hoge)],
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

    override def fragments[U <: DataModel[U]]: (Fragment[Hoge], Map[BranchKey, OutputFragment[U]]) = {
      val fragment = new HogeOutputFragment
      val outputs = Map(HogeResult -> fragment)
      (fragment, outputs.asInstanceOf[Map[BranchKey, OutputFragment[U]]])
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
