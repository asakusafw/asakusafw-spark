package com.asakusafw.spark.runtime
package driver

import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner

import org.apache.hadoop.conf.Configuration
import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import com.asakusafw.runtime.model.DataModel
import com.asakusafw.runtime.value.IntOption
import com.asakusafw.spark.runtime.aggregation.Aggregation
import com.asakusafw.spark.runtime.fragment._

@RunWith(classOf[JUnitRunner])
class MapDriverSpecTest extends MapDriverSpec

class MapDriverSpec extends FlatSpec with SparkSugar {

  import MapDriverSpec._

  behavior of classOf[MapDriver[_, _]].getSimpleName

  it should "map" in {
    val hoges = sc.parallelize(0 until 10).map { i =>
      val hoge = new Hoge()
      hoge.id.modify(i)
      (new ShuffleKey(Seq.empty, Seq.empty) {}, hoge)
    }.asInstanceOf[RDD[(ShuffleKey, Hoge)]]

    val driver = new TestMapDriver(sc, hadoopConf, hoges)

    val outputs = driver.execute()
    outputs.mapValues(_.collect.toSeq).foreach {
      case (HogeResult, values) =>
        val hogeResult = values.asInstanceOf[Seq[(_, Hoge)]].map(_._2)
        assert(hogeResult.size === 10)
        assert(hogeResult.map(_.id.get) === (0 until 10))
      case (FooResult, values) =>
        val fooResult = values.asInstanceOf[Seq[(_, Foo)]].map(_._2)
        assert(fooResult.size === 45)
        assert(fooResult.map(foo => (foo.id.get, foo.hogeId.get)) ===
          (for {
            i <- (0 until 10)
            j <- (0 until i)
          } yield {
            ((i * (i - 1)) / 2 + j, i)
          }))
    }
  }
}

object MapDriverSpec {

  val HogeResult = BranchKey(0)
  val FooResult = BranchKey(1)

  class TestMapDriver(
    @transient sc: SparkContext,
    @transient hadoopConf: Broadcast[Configuration],
    @transient prev: RDD[(ShuffleKey, Hoge)])
      extends MapDriver[Hoge, String](sc, hadoopConf, Map.empty, Seq(prev)) {

    override def name = "TestMap"

    override def branchKeys: Set[BranchKey] = Set(HogeResult, FooResult)

    override def partitioners: Map[BranchKey, Partitioner] = Map.empty

    override def orderings: Map[BranchKey, Ordering[ShuffleKey]] = Map.empty

    override def aggregations: Map[BranchKey, Aggregation[ShuffleKey, _, _]] = Map.empty

    override def fragments[U <: DataModel[U]]: (Fragment[Hoge], Map[BranchKey, OutputFragment[U]]) = {
      val outputs = Map(
        HogeResult -> new HogeOutputFragment,
        FooResult -> new FooOutputFragment)
      val fragment = new TestFragment(outputs)
      (fragment, outputs.asInstanceOf[Map[BranchKey, OutputFragment[U]]])
    }

    override def shuffleKey(branch: BranchKey, value: Any): ShuffleKey = null
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

  class TestFragment(outputs: Map[BranchKey, Fragment[_]]) extends Fragment[Hoge] {

    private val foo = new Foo()

    override def add(hoge: Hoge): Unit = {
      outputs(HogeResult).asInstanceOf[HogeOutputFragment].add(hoge)
      for (i <- 0 until hoge.id.get) {
        foo.reset()
        foo.id.modify((hoge.id.get * (hoge.id.get - 1)) / 2 + i)
        foo.hogeId.copyFrom(hoge.id)
        outputs(FooResult).asInstanceOf[FooOutputFragment].add(foo)
      }
    }

    override def reset(): Unit = {
      outputs.values.foreach(_.reset())
    }
  }
}
