package com.asakusafw.spark.runtime
package driver

import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner

import scala.reflect.ClassTag

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Writable
import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd._

import com.asakusafw.runtime.model.DataModel
import com.asakusafw.runtime.value.{ BooleanOption, IntOption }
import com.asakusafw.spark.runtime.aggregation.Aggregation
import com.asakusafw.spark.runtime.fragment._
import com.asakusafw.spark.runtime.orderings._

@RunWith(classOf[JUnitRunner])
class CoGroupDriverSpecTest extends CoGroupDriverSpec

class CoGroupDriverSpec extends FlatSpec with SparkSugar {

  import CoGroupDriverSpec._

  behavior of "CoGroupDriver"

  it should "cogroup" in {
    val hogeOrd = Seq(true)
    val hoges = sc.parallelize(0 until 10).map { i =>
      val hoge = new Hoge()
      hoge.id.modify(i)
      (new ShuffleKey(Seq(hoge.id), Seq(new BooleanOption().modify(hoge.id.get % 3 == 0))) {}, hoge)
    }.asInstanceOf[RDD[(ShuffleKey, _)]]

    val fooOrd = Seq(true)
    val foos = sc.parallelize(0 until 10).flatMap(i => (0 until i).map { j =>
      val foo = new Foo()
      foo.id.modify(10 + j)
      foo.hogeId.modify(i)
      (new ShuffleKey(Seq(foo.hogeId), Seq(new IntOption().modify(foo.id.toString.hashCode))) {}, foo)
    }).asInstanceOf[RDD[(ShuffleKey, _)]]

    val part = new HashPartitioner(2)
    val driver = new TestCoGroupDriver(
      sc, hadoopConf, Seq((Seq(hoges), hogeOrd), (Seq(foos), fooOrd)), part)

    val outputs = driver.execute()
    outputs.mapValues(_.collect.toSeq).foreach {
      case ("hogeResult", values) =>
        val hogeResults = values.asInstanceOf[Seq[(ShuffleKey, Hoge)]]
        assert(hogeResults.size === 1)
        assert(hogeResults(0)._2.id.get === 1)
      case ("fooResult", values) =>
        val fooResults = values.asInstanceOf[Seq[(ShuffleKey, Foo)]]
        assert(fooResults.size === 1)
        assert(fooResults(0)._2.id.get === 10)
        assert(fooResults(0)._2.hogeId.get === 1)
      case ("hogeError", values) =>
        val hogeErrors = values.asInstanceOf[Seq[(ShuffleKey, Hoge)]].sortBy(_._2.id)
        assert(hogeErrors.size === 9)
        assert(hogeErrors(0)._2.id.get === 0)
        for (i <- 2 until 10) {
          assert(hogeErrors(i - 1)._2.id.get === i)
        }
      case ("fooError", values) =>
        val fooErrors = values.asInstanceOf[Seq[(ShuffleKey, Foo)]].sortBy(_._2.hogeId)
        assert(fooErrors.size === 44)
        for {
          i <- 2 until 10
          j <- 0 until i
        } {
          assert(fooErrors((i * (i - 1)) / 2 + j - 1)._2.id.get == 10 + j)
          assert(fooErrors((i * (i - 1)) / 2 + j - 1)._2.hogeId.get == i)
        }
    }
  }
}

object CoGroupDriverSpec {

  class TestCoGroupDriver(
    @transient sc: SparkContext,
    @transient hadoopConf: Broadcast[Configuration],
    @transient inputs: Seq[(Seq[RDD[(ShuffleKey, _)]], Seq[Boolean])],
    @transient part: Partitioner)
      extends CoGroupDriver[String](sc, hadoopConf, Map.empty, inputs, part) {

    override def name = "TestCoGroup"

    override def branchKeys: Set[String] = {
      Set("hogeResult", "fooResult", "hogeError", "fooError")
    }

    override def partitioners: Map[String, Partitioner] = Map.empty

    override def orderings: Map[String, Ordering[ShuffleKey]] = Map.empty

    override def aggregations: Map[String, Aggregation[ShuffleKey, _, _]] = Map.empty

    override def fragments[U <: DataModel[U]]: (Fragment[Seq[Iterable[_]]], Map[String, OutputFragment[U]]) = {
      val outputs = Map(
        "hogeResult" -> new HogeOutputFragment,
        "fooResult" -> new FooOutputFragment,
        "hogeError" -> new HogeOutputFragment,
        "fooError" -> new FooOutputFragment)
      val fragment = new TestCoGroupFragment(outputs)
      (fragment, outputs.asInstanceOf[Map[String, OutputFragment[U]]])
    }

    override def shuffleKey(branch: String, value: Any): ShuffleKey = null
  }

  class Hoge extends DataModel[Hoge] {

    val id = new IntOption()

    override def reset(): Unit = {
      id.setNull()
    }
    override def copyFrom(other: Hoge): Unit = {
      id.copyFrom(other.id)
    }
  }

  class Foo extends DataModel[Foo] {

    val id = new IntOption()
    val hogeId = new IntOption()

    override def reset(): Unit = {
      id.setNull()
      hogeId.setNull()
    }
    override def copyFrom(other: Foo): Unit = {
      id.copyFrom(other.id)
      hogeId.copyFrom(other.hogeId)
    }
  }

  class HogeOutputFragment extends OutputFragment[Hoge] {
    override def newDataModel: Hoge = new Hoge()
  }

  class FooOutputFragment extends OutputFragment[Foo] {
    override def newDataModel: Foo = new Foo()
  }

  class GroupingPartitioner(val numPartitions: Int) extends Partitioner {

    override def getPartition(key: Any): Int = {
      val shuffleKey = key.asInstanceOf[ShuffleKey]
      val part = shuffleKey.grouping.hashCode % numPartitions
      if (part < 0) part + numPartitions else part
    }
  }

  class TestCoGroupFragment(outputs: Map[String, Fragment[_]]) extends Fragment[Seq[Iterable[_]]] {

    override def add(groups: Seq[Iterable[_]]): Unit = {
      assert(groups.size == 2)
      val hogeList = groups(0).asInstanceOf[Iterable[Hoge]].toSeq
      val fooList = groups(1).asInstanceOf[Iterable[Foo]].toSeq
      if (hogeList.size == 1 && fooList.size == 1) {
        outputs("hogeResult").asInstanceOf[HogeOutputFragment].add(hogeList(0))
        outputs("fooResult").asInstanceOf[FooOutputFragment].add(fooList(0))
      } else {
        hogeList.foreach(outputs("hogeError").asInstanceOf[HogeOutputFragment].add)
        fooList.foreach(outputs("fooError").asInstanceOf[FooOutputFragment].add)
      }
    }

    override def reset(): Unit = {
      outputs.values.foreach(_.reset())
    }
  }
}
