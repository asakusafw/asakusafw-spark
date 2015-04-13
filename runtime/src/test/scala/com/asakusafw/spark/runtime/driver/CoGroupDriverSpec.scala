package com.asakusafw.spark.runtime
package driver

import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner

import scala.reflect.ClassTag

import org.apache.hadoop.conf.Configuration
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
    val fHoge = new Function1[Int, (ShuffleKey, Hoge)] with Serializable {
      @transient var h: Hoge = _
      def hoge: Hoge = {
        if (h == null) {
          h = new Hoge()
        }
        h
      }
      @transient var sk: ShuffleKey = _
      def shuffleKey: ShuffleKey = {
        if (sk == null) {
          sk = new ShuffleKey(Seq(new IntOption()), Seq(new BooleanOption()))
        }
        sk
      }
      override def apply(i: Int): (ShuffleKey, Hoge) = {
        hoge.id.modify(i)
        shuffleKey.grouping(0).asInstanceOf[IntOption].copyFrom(hoge.id)
        shuffleKey.ordering(0).asInstanceOf[BooleanOption].modify(hoge.id.get % 3 == 0)
        (shuffleKey, hoge)
      }
    }
    val hoges = sc.parallelize(0 until 100).map(fHoge).asInstanceOf[RDD[(ShuffleKey, _)]]

    val fooOrd = Seq(true)
    val fFoo = new Function2[Int, Int, (ShuffleKey, Foo)] with Serializable {
      @transient var f: Foo = _
      def foo: Foo = {
        if (f == null) {
          f = new Foo()
        }
        f
      }
      @transient var sk: ShuffleKey = _
      def shuffleKey: ShuffleKey = {
        if (sk == null) {
          sk = new ShuffleKey(Seq(new IntOption()), Seq(new IntOption()))
        }
        sk
      }
      override def apply(i: Int, j: Int): (ShuffleKey, Foo) = {
        foo.id.modify(100 + j)
        foo.hogeId.modify(i)
        shuffleKey.grouping(0).asInstanceOf[IntOption].copyFrom(foo.hogeId)
        shuffleKey.ordering(0).asInstanceOf[IntOption].modify(foo.id.toString.hashCode)
        (shuffleKey, foo)
      }
    }
    val foos = sc.parallelize(0 until 100).flatMap(i => (0 until i).iterator.map(fFoo(i, _)))
      .asInstanceOf[RDD[(ShuffleKey, _)]]

    val part = new HashPartitioner(2)
    val driver = new TestCoGroupDriver(
      sc, hadoopConf, Seq((Seq(hoges), hogeOrd), (Seq(foos), fooOrd)), part)

    val outputs = driver.execute()
    outputs.mapValues(_.collect.toSeq).foreach {
      case (HogeResult, values) =>
        val hogeResults = values.asInstanceOf[Seq[(ShuffleKey, Hoge)]]
        assert(hogeResults.size === 1)
        assert(hogeResults.head._2.id.get === 1)
      case (FooResult, values) =>
        val fooResults = values.asInstanceOf[Seq[(ShuffleKey, Foo)]]
        assert(fooResults.size === 1)
        assert(fooResults.head._2.id.get === 100)
        assert(fooResults.head._2.hogeId.get === 1)
      case (HogeError, values) =>
        val hogeErrors = values.asInstanceOf[Seq[(ShuffleKey, Hoge)]].sortBy(_._2.id.get)
        assert(hogeErrors.size === 99)
        assert(hogeErrors.head._2.id.get === 0)
        for (i <- 2 until 10) {
          assert(hogeErrors(i - 1)._2.id.get === i)
        }
      case (FooError, values) =>
        val fooErrors = values.asInstanceOf[Seq[(ShuffleKey, Foo)]].sortBy(_._2.hogeId.get)
        assert(fooErrors.size === 4949)
        for {
          i <- 2 until 100
          j <- 0 until i
        } {
          assert(fooErrors((i * (i - 1)) / 2 + j - 1)._2.id.get == 100 + j)
          assert(fooErrors((i * (i - 1)) / 2 + j - 1)._2.hogeId.get == i)
        }
    }
  }
}

object CoGroupDriverSpec {

  val HogeResult = BranchKey(0)
  val FooResult = BranchKey(1)
  val HogeError = BranchKey(2)
  val FooError = BranchKey(3)

  class TestCoGroupDriver(
    @transient sc: SparkContext,
    @transient hadoopConf: Broadcast[Configuration],
    @transient inputs: Seq[(Seq[RDD[(ShuffleKey, _)]], Seq[Boolean])],
    @transient part: Partitioner)
      extends CoGroupDriver(sc, hadoopConf, Map.empty, inputs, part) {

    override def name = "TestCoGroup"

    override def branchKeys: Set[BranchKey] = {
      Set(HogeResult, FooResult, HogeError, FooError)
    }

    override def partitioners: Map[BranchKey, Partitioner] = Map.empty

    override def orderings: Map[BranchKey, Ordering[ShuffleKey]] = Map.empty

    override def aggregations: Map[BranchKey, Aggregation[ShuffleKey, _, _]] = Map.empty

    override def fragments: (Fragment[Seq[Iterable[_]]], Map[BranchKey, OutputFragment[_]]) = {
      val outputs = Map(
        HogeResult -> new HogeOutputFragment,
        FooResult -> new FooOutputFragment,
        HogeError -> new HogeOutputFragment,
        FooError -> new FooOutputFragment)
      val fragment = new TestCoGroupFragment(outputs)
      (fragment, outputs)
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

  class TestCoGroupFragment(outputs: Map[BranchKey, Fragment[_]]) extends Fragment[Seq[Iterable[_]]] {

    override def add(groups: Seq[Iterable[_]]): Unit = {
      assert(groups.size == 2)
      val hogeList = groups(0).asInstanceOf[Iterable[Hoge]].toSeq
      val fooList = groups(1).asInstanceOf[Iterable[Foo]].toSeq
      if (hogeList.size == 1 && fooList.size == 1) {
        outputs(HogeResult).asInstanceOf[HogeOutputFragment].add(hogeList.head)
        outputs(FooResult).asInstanceOf[FooOutputFragment].add(fooList.head)
      } else {
        hogeList.foreach(outputs(HogeError).asInstanceOf[HogeOutputFragment].add)
        fooList.foreach(outputs(FooError).asInstanceOf[FooOutputFragment].add)
      }
    }

    override def reset(): Unit = {
      outputs.values.foreach(_.reset())
    }
  }
}
