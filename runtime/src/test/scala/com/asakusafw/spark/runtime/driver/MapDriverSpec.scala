package com.asakusafw.spark.runtime
package driver

import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.NullWritable
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import com.asakusafw.runtime.model.DataModel
import com.asakusafw.runtime.value.{ BooleanOption, IntOption }
import com.asakusafw.spark.runtime.aggregation.Aggregation
import com.asakusafw.spark.runtime.fragment._
import com.asakusafw.spark.runtime.rdd.BranchKey

@RunWith(classOf[JUnitRunner])
class MapDriverSpecTest extends MapDriverSpec

class MapDriverSpec extends FlatSpec with SparkSugar {

  import MapDriverSpec._

  behavior of classOf[MapDriver[_]].getSimpleName

  it should "map simply" in {
    import Simple._
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
        (NullWritable.get, hoge)
      }
    }
    val hoges = sc.parallelize(0 until 100).map(f)

    val driver = new SimpleMapDriver(sc, hadoopConf, hoges)

    val outputs = driver.execute()
    val hogeResult = outputs(HogeResult).map(_._2.asInstanceOf[Hoge].id.get).collect.toSeq
    assert(hogeResult.size === 100)
    assert(hogeResult === (0 until 100))
  }

  it should "map with branch" in {
    import Branch._
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
        (NullWritable.get, hoge)
      }
    }
    val hoges = sc.parallelize(0 until 100).map(f)

    val driver = new BranchMapDriver(sc, hadoopConf, hoges)

    val outputs = driver.execute()
    val hoge1Result = outputs(Hoge1Result).map(_._2.asInstanceOf[Hoge]).collect.toSeq
    assert(hoge1Result.size === 50)
    assert(hoge1Result.map(_.id.get) === (0 until 100 by 2))
    val hoge2Result = outputs(Hoge2Result).map(_._2.asInstanceOf[Hoge]).collect.toSeq
    assert(hoge2Result.size === 50)
    assert(hoge2Result.map(_.id.get) === (1 until 100 by 2))
  }

  it should "map with branch and ordering" in {
    import BranchAndOrdering._
    val f = new Function1[Int, (_, Foo)] with Serializable {
      @transient var f: Foo = _
      def foo: Foo = {
        if (f == null) {
          f = new Foo()
        }
        f
      }
      override def apply(i: Int): (_, Foo) = {
        foo.id.modify(i % 5)
        foo.ord.modify(i)
        (NullWritable.get, foo)
      }
    }
    val hoges = sc.parallelize(0 until 100).map(f)

    val driver = new BranchAndOrderingMapDriver(sc, hadoopConf, hoges)

    val outputs = driver.execute()
    val foo1Result = outputs(Foo1Result).map(_._2.asInstanceOf[Foo]).collect.toSeq
    assert(foo1Result.size === 40)
    assert(foo1Result.map(_.id.get) === (0 until 100).map(_ % 5).filter(_ % 3 == 0))
    assert(foo1Result.map(_.ord.get) === (0 until 100).filter(i => (i % 5) % 3 == 0))
    val foo2Result = outputs(Foo2Result).map(_._2.asInstanceOf[Foo]).collect.toSeq
    assert(foo2Result.size === 60)
    assert(foo2Result.map(foo => (foo.id.get, foo.ord.get)) ===
      (0 until 100).filterNot(i => (i % 5) % 3 == 0).map(i => (i % 5, i)).sortBy(t => (t._1, -t._2)))
  }
}

object MapDriverSpec {

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
    val ord = new IntOption()

    override def reset(): Unit = {
      id.setNull()
      ord.setNull()
    }
    override def copyFrom(other: Foo): Unit = {
      id.copyFrom(other.id)
      ord.copyFrom(other.ord)
    }
  }

  class HogeOutputFragment extends OutputFragment[Hoge] {
    override def newDataModel: Hoge = new Hoge()
  }

  class FooOutputFragment extends OutputFragment[Foo] {
    override def newDataModel: Foo = new Foo()
  }

  object Simple {

    val HogeResult = BranchKey(0)

    class SimpleMapDriver(
      @transient sc: SparkContext,
      @transient hadoopConf: Broadcast[Configuration],
      @transient prev: RDD[(_, Hoge)])
        extends MapDriver[Hoge](sc, hadoopConf, Map.empty, Seq(prev)) {

      override def name = "SimpleMap"

      override def branchKeys: Set[BranchKey] = Set(HogeResult)

      override def partitioners: Map[BranchKey, Partitioner] = Map.empty

      override def orderings: Map[BranchKey, Ordering[ShuffleKey]] = Map.empty

      override def aggregations: Map[BranchKey, Aggregation[ShuffleKey, _, _]] = Map.empty

      override def fragments: (Fragment[Hoge], Map[BranchKey, OutputFragment[_]]) = {
        val output = new HogeOutputFragment
        val fragment = new SimpleFragment(output)
        (fragment, Map(HogeResult -> output))
      }

      override def shuffleKey(branch: BranchKey, value: Any): ShuffleKey = null
    }

    class SimpleFragment(output: Fragment[Hoge]) extends Fragment[Hoge] {

      override def add(hoge: Hoge): Unit = {
        output.add(hoge)
      }

      override def reset(): Unit = {
        output.reset()
      }
    }
  }

  object Branch {

    val Hoge1Result = BranchKey(0)
    val Hoge2Result = BranchKey(1)

    class BranchMapDriver(
      @transient sc: SparkContext,
      @transient hadoopConf: Broadcast[Configuration],
      @transient prev: RDD[(_, Hoge)])
        extends MapDriver[Hoge](sc, hadoopConf, Map.empty, Seq(prev)) {

      override def name = "BranchMap"

      override def branchKeys: Set[BranchKey] = Set(Hoge1Result, Hoge2Result)

      override def partitioners: Map[BranchKey, Partitioner] = Map.empty

      override def orderings: Map[BranchKey, Ordering[ShuffleKey]] = Map.empty

      override def aggregations: Map[BranchKey, Aggregation[ShuffleKey, _, _]] = Map.empty

      override def fragments: (Fragment[Hoge], Map[BranchKey, OutputFragment[_]]) = {
        val hoge1Output = new HogeOutputFragment
        val hoge2Output = new HogeOutputFragment
        val fragment = new BranchFragment(hoge1Output, hoge2Output)
        (fragment,
          Map(
            Hoge1Result -> hoge1Output,
            Hoge2Result -> hoge2Output))
      }

      override def shuffleKey(branch: BranchKey, value: Any): ShuffleKey = null
    }

    class BranchFragment(hoge1Output: Fragment[Hoge], hoge2Output: Fragment[Hoge]) extends Fragment[Hoge] {

      override def add(hoge: Hoge): Unit = {
        if (hoge.id.get % 2 == 0) {
          hoge1Output.add(hoge)
        } else {
          hoge2Output.add(hoge)
        }
      }

      override def reset(): Unit = {
        hoge1Output.reset()
        hoge2Output.reset()
      }
    }
  }

  object BranchAndOrdering {

    val Foo1Result = BranchKey(0)
    val Foo2Result = BranchKey(1)

    class BranchAndOrderingMapDriver(
      @transient sc: SparkContext,
      @transient hadoopConf: Broadcast[Configuration],
      @transient prev: RDD[(_, Foo)])
        extends MapDriver[Foo](sc, hadoopConf, Map.empty, Seq(prev)) {

      override def name = "BranchAndOrderingMap"

      override def branchKeys: Set[BranchKey] = Set(Foo1Result, Foo2Result)

      override def partitioners: Map[BranchKey, Partitioner] =
        Map(Foo2Result -> new HashPartitioner(1))

      override def orderings: Map[BranchKey, Ordering[ShuffleKey]] =
        Map(Foo2Result -> new ShuffleKey.SortOrdering(Array(false)))

      override def aggregations: Map[BranchKey, Aggregation[ShuffleKey, _, _]] = Map.empty

      override def fragments: (Fragment[Foo], Map[BranchKey, OutputFragment[_]]) = {
        val foo1Output = new FooOutputFragment
        val foo2Output = new FooOutputFragment
        val fragment = new BranchFragment(foo1Output, foo2Output)
        (fragment,
          Map(
            Foo1Result -> foo1Output,
            Foo2Result -> foo2Output))
      }

      @transient var sk: ShuffleKey = _

      def shuffleKey = {
        if (sk == null) {
          sk = new ShuffleKey(Seq(new IntOption()), Seq(new IntOption()))
        }
        sk
      }

      override def shuffleKey(branch: BranchKey, value: Any): ShuffleKey = {
        val foo = value.asInstanceOf[Foo]
        shuffleKey.grouping(0).asInstanceOf[IntOption].copyFrom(foo.id)
        shuffleKey.ordering(0).asInstanceOf[IntOption].copyFrom(foo.ord)
        shuffleKey
      }
    }

    class BranchFragment(foo1Output: Fragment[Foo], foo2Output: Fragment[Foo]) extends Fragment[Foo] {

      override def add(foo: Foo): Unit = {
        if (foo.id.get % 3 == 0) {
          foo1Output.add(foo)
        } else {
          foo2Output.add(foo)
        }
      }

      override def reset(): Unit = {
        foo1Output.reset()
        foo2Output.reset()
      }
    }
  }
}
