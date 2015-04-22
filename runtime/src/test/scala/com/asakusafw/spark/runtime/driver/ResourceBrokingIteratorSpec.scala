package com.asakusafw.spark.runtime
package driver

import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner

import scala.reflect.ClassTag

import org.apache.hadoop.conf.Configuration
import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import com.asakusafw.bridge.api.BatchContext
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.runtime.value.{ IntOption, StringOption }
import com.asakusafw.spark.runtime.aggregation.Aggregation
import com.asakusafw.spark.runtime.fragment._
import com.asakusafw.spark.runtime.rdd.BranchKey

@RunWith(classOf[JUnitRunner])
class ResourceBrokingIteratorSpecTest extends ResourceBrokingIteratorSpec

class ResourceBrokingIteratorSpec extends FlatSpec with SparkSugar {

  import ResourceBrokingIteratorSpec._

  behavior of classOf[ResourceBrokingIterator[_]].getSimpleName

  it should "broke resources" in {
    val hoges = sc.parallelize(0 until 10).map { i =>
      val hoge = new Hoge()
      hoge.id.modify(i)
      ((), hoge)
    }.asInstanceOf[RDD[(_, Hoge)]]

    val driver = new TestDriver(sc, hadoopConf, hoges)

    val outputs = driver.execute()

    val result = outputs(Result).map(_._2.asInstanceOf[Hoge])
      .map(hoge => (hoge.id.get, hoge.str.getAsString)).collect.toSeq
    assert(result.size === 10)
    assert(result.map(_._1) === (0 until 10))
    assert(result.map(_._2) === (0 until 10).map(i => s"test_${i}"))
  }
}

object ResourceBrokingIteratorSpec {

  val Result = BranchKey(0)

  class TestDriver(
    @transient sc: SparkContext,
    @transient hadoopConf: Broadcast[Configuration],
    @transient prev: RDD[(_, Hoge)])
      extends MapDriver[Hoge](sc, hadoopConf, Map.empty, Seq(prev)) {

    override def name = "TestMap"

    override def branchKeys: Set[BranchKey] = Set(Result)

    override def partitioners: Map[BranchKey, Partitioner] = Map.empty

    override def orderings: Map[BranchKey, Ordering[ShuffleKey]] = Map.empty

    override def aggregations: Map[BranchKey, Aggregation[ShuffleKey, _, _]] = Map.empty

    override def fragments: (Fragment[Hoge], Map[BranchKey, OutputFragment[_]]) = {
      val outputs = Map(
        Result -> new HogeOutputFragment)
      val fragment = new TestFragment(outputs(Result))
      (fragment, outputs)
    }

    override def shuffleKey(branch: BranchKey, value: Any): ShuffleKey = null
  }

  class Hoge extends DataModel[Hoge] {

    val id = new IntOption()
    val str = new StringOption()

    override def reset(): Unit = {
      id.setNull()
      str.setNull()
    }
    override def copyFrom(other: Hoge): Unit = {
      id.copyFrom(other.id)
      str.copyFrom(other.str)
    }
  }

  class HogeOutputFragment extends OutputFragment[Hoge] {
    override def newDataModel: Hoge = new Hoge()
  }

  class TestFragment(output: Fragment[Hoge]) extends Fragment[Hoge] {

    override def add(hoge: Hoge): Unit = {
      hoge.str.modify(s"${BatchContext.get("batcharg")}_${hoge.id.get}")
      output.add(hoge)
    }

    override def reset(): Unit = {
      output.reset()
    }
  }
}
