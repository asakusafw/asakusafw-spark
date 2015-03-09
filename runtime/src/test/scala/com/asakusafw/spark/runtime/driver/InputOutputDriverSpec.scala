package com.asakusafw.spark.runtime
package driver

import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner

import java.io.{ DataInput, DataOutput, File }

import scala.reflect.ClassTag

import org.apache.hadoop.io.Writable
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._

import com.asakusafw.runtime.model.DataModel
import com.asakusafw.runtime.value.IntOption
import com.asakusafw.spark.runtime.fragment._
import com.asakusafw.spark.runtime.orderings._

@RunWith(classOf[JUnitRunner])
class InputOutputDriverSpecTest extends InputOutputDriverSpec

class InputOutputDriverSpec extends FlatSpec with SparkSugar {

  import InputOutputDriverSpec._

  behavior of "Input/OutputDriver"

  it should "output and input" in {
    val tmpDir = File.createTempFile("test-", null)
    tmpDir.delete
    val path = tmpDir.getAbsolutePath

    val hoges = sc.parallelize(0 until 10).map { i =>
      val hoge = new Hoge()
      hoge.id.modify(i)
      (hoge, hoge)
    }

    new TestOutputDriver(sc, hoges.asInstanceOf[RDD[(_, Hoge)]], path).execute()

    val inputs = new TestInputDriver(sc, path).execute()
    assert(inputs("hogeResult").map(_._2.asInstanceOf[Hoge].id.get).collect.toSeq === (0 until 10))
  }
}

object InputOutputDriverSpec {

  class TestOutputDriver(
    @transient sc: SparkContext,
    @transient input: RDD[(_, Hoge)],
    val path: String)
      extends OutputDriver[Hoge](sc, input)

  class TestInputDriver(
    @transient sc: SparkContext,
    basePath: String)
      extends InputDriver[Hoge, String](sc) {

    override def paths: Set[String] = Set(basePath + "/part-*")

    override def branchKey: String = {
      "hogeResult"
    }

    override def shuffleKey[U](branch: String, value: DataModel[_]): U =
      value.asInstanceOf[Hoge].id.get.asInstanceOf[U]
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
}
