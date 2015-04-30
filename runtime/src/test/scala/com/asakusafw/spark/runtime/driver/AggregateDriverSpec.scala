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

import com.asakusafw.runtime.model.DataModel
import com.asakusafw.runtime.value.IntOption
import com.asakusafw.spark.runtime.aggregation.Aggregation
import com.asakusafw.spark.runtime.fragment._
import com.asakusafw.spark.runtime.rdd.BranchKey

@RunWith(classOf[JUnitRunner])
class AggregateDriverSpecTest extends AggregateDriverSpec

class AggregateDriverSpec extends FlatSpec with SparkSugar {

  import AggregateDriverSpec._

  behavior of classOf[AggregateDriver[_, _]].getSimpleName

  it should "aggregate with map-side combine" in {
    import TotalAggregate._
    val f = new Function1[Int, (ShuffleKey, Hoge)] with Serializable {
      @transient var h: Hoge = _
      def hoge: Hoge = {
        if (h == null) {
          h = new Hoge()
        }
        h
      }
      override def apply(i: Int): (ShuffleKey, Hoge) = {
        hoge.id.modify(i % 2)
        hoge.price.modify(i * 100)
        val shuffleKey = new ShuffleKey(Seq(new IntOption()), Seq(new IntOption()))
        shuffleKey.grouping(0).asInstanceOf[IntOption].copyFrom(hoge.id)
        shuffleKey.ordering(0).asInstanceOf[IntOption].copyFrom(hoge.price)
        (shuffleKey, hoge)
      }
    }
    val hoges = sc.parallelize(0 until 10).map(f)

    val part = new HashPartitioner(2)
    val aggregation = new TestAggregation(true)

    val driver = new TestAggregateDriver(
      sc, hadoopConf, hoges, Option(new ShuffleKey.SortOrdering(1, Array(false))), part, aggregation)

    val outputs = driver.execute()
    assert(outputs(Result).map {
      case (id: ShuffleKey, hoge: Hoge) => (id.grouping(0).asInstanceOf[IntOption].get, hoge.price.get)
    }.collect.toSeq.sortBy(_._1) ===
      Seq((0, (0 until 10 by 2).map(_ * 100).sum), (1, (1 until 10 by 2).map(_ * 100).sum)))
  }

  it should "aggregate without map-side combine" in {
    import TotalAggregate._
    val f = new Function1[Int, (ShuffleKey, Hoge)] with Serializable {
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
          sk = new ShuffleKey(Seq(new IntOption()), Seq(new IntOption()))
        }
        sk
      }
      override def apply(i: Int): (ShuffleKey, Hoge) = {
        hoge.id.modify(i % 2)
        hoge.price.modify(i * 100)
        shuffleKey.grouping(0).asInstanceOf[IntOption].copyFrom(hoge.id)
        shuffleKey.ordering(0).asInstanceOf[IntOption].copyFrom(hoge.price)
        (shuffleKey, hoge)
      }
    }
    val hoges = sc.parallelize(0 until 10).map(f)

    val part = new HashPartitioner(2)
    val aggregation = new TestAggregation(false)

    val driver = new TestAggregateDriver(
      sc, hadoopConf, hoges, Option(new ShuffleKey.SortOrdering(1, Array(false))), part, aggregation)

    val outputs = driver.execute()
    assert(outputs(Result).map {
      case (id: ShuffleKey, hoge: Hoge) => (id.grouping(0).asInstanceOf[IntOption].get, hoge.price.get)
    }.collect.toSeq.sortBy(_._1) ===
      Seq((0, (0 until 10 by 2).map(_ * 100).sum), (1, (1 until 10 by 2).map(_ * 100).sum)))
  }

  it should "aggregate partially" in {
    import PartialAggregate._
    val f = new Function1[Int, (ShuffleKey, Hoge)] with Serializable {
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
          sk = new ShuffleKey(Seq(new IntOption()), Seq(new IntOption()))
        }
        sk
      }
      override def apply(i: Int): (ShuffleKey, Hoge) = {
        hoge.id.modify(i % 2)
        hoge.price.modify(i * 100)
        shuffleKey.grouping(0).asInstanceOf[IntOption].copyFrom(hoge.id)
        shuffleKey.ordering(0).asInstanceOf[IntOption].copyFrom(hoge.price)
        (shuffleKey, hoge)
      }
    }
    val hoges = sc.parallelize(0 until 10, 2).map(f)

    val driver = new TestPartialAggregationMapDriver(sc, hadoopConf, hoges.asInstanceOf[RDD[(_, Hoge)]])

    val outputs = driver.execute()
    assert(outputs(Result1).map {
      case (id: ShuffleKey, hoge: Hoge) => (id.grouping(0).asInstanceOf[IntOption].get, hoge.price.get)
    }.collect.toSeq.sortBy(_._1) ===
      Seq(
        (0, (0 until 10).filter(_ % 2 == 0).filter(_ < 5).map(_ * 100).sum),
        (0, (0 until 10).filter(_ % 2 == 0).filterNot(_ < 5).map(_ * 100).sum),
        (1, (0 until 10).filter(_ % 2 == 1).filter(_ < 5).map(_ * 100).sum),
        (1, (0 until 10).filter(_ % 2 == 1).filterNot(_ < 5).map(_ * 100).sum)))
    assert(outputs(Result2).collect.toSeq.map { case (key, hoge: Hoge) => hoge.price.get } === (0 until 10).map(100 * _))
  }
}

object AggregateDriverSpec {

  class Hoge extends DataModel[Hoge] {

    val id = new IntOption()
    val price = new IntOption()

    override def reset(): Unit = {
      id.setNull()
      price.setNull()
    }
    override def copyFrom(other: Hoge): Unit = {
      id.copyFrom(other.id)
      price.copyFrom(other.price)
    }
  }

  class HogeOutputFragment extends OutputFragment[Hoge] {
    override def newDataModel: Hoge = new Hoge()
  }

  class TestAggregation(val mapSideCombine: Boolean)
      extends Aggregation[ShuffleKey, Hoge, Hoge] {

    override def createCombiner(value: Hoge): Hoge = {
      val combiner = new Hoge()
      combiner.copyFrom(value)
      combiner
    }

    override def mergeValue(combiner: Hoge, value: Hoge): Hoge = {
      combiner.price.add(value.price)
      combiner
    }

    override def mergeCombiners(comb1: Hoge, comb2: Hoge): Hoge = {
      comb1.price.add(comb2.price)
      comb1
    }
  }

  object TotalAggregate {
    val Result = BranchKey(0)

    class TestAggregateDriver(
      @transient sc: SparkContext,
      @transient hadoopConf: Broadcast[Configuration],
      @transient prev: RDD[(ShuffleKey, Hoge)],
      @transient sort: Option[ShuffleKey.SortOrdering],
      @transient part: Partitioner,
      val aggregation: Aggregation[ShuffleKey, Hoge, Hoge])
        extends AggregateDriver[Hoge, Hoge](sc, hadoopConf, Map.empty, Seq(prev), sort, part) {

      override def name = "TestAggregation"

      override def branchKeys: Set[BranchKey] = Set(Result)

      override def partitioners: Map[BranchKey, Partitioner] = Map.empty

      override def orderings: Map[BranchKey, Ordering[ShuffleKey]] = Map.empty

      override def aggregations: Map[BranchKey, Aggregation[ShuffleKey, _, _]] = Map.empty

      override def fragments(broadcasts: Map[BroadcastId, Broadcast[_]]): (Fragment[Hoge], Map[BranchKey, OutputFragment[_]]) = {
        val fragment = new HogeOutputFragment
        val outputs = Map(Result -> fragment)
        (fragment, outputs)
      }

      @transient var sk: ShuffleKey = _

      def shuffleKey = {
        if (sk == null) {
          sk = new ShuffleKey(Seq(new IntOption()), Seq.empty)
        }
        sk
      }

      override def shuffleKey(branch: BranchKey, value: Any): ShuffleKey = {
        shuffleKey.grouping(0).asInstanceOf[IntOption].copyFrom(value.asInstanceOf[Hoge].id)
        shuffleKey
      }
    }
  }

  object PartialAggregate {
    val Result1 = BranchKey(0)
    val Result2 = BranchKey(1)

    class TestPartialAggregationMapDriver(
      @transient sc: SparkContext,
      @transient hadoopConf: Broadcast[Configuration],
      @transient prev: RDD[(_, Hoge)])
        extends MapDriver[Hoge](sc, hadoopConf, Map.empty, Seq(prev)) {

      override def name = "TestPartialAggregation"

      override def branchKeys: Set[BranchKey] = Set(Result1, Result2)

      override def partitioners: Map[BranchKey, Partitioner] = {
        Map(Result1 -> new HashPartitioner(2))
      }

      override def orderings: Map[BranchKey, Ordering[ShuffleKey]] = Map.empty

      override def aggregations: Map[BranchKey, Aggregation[ShuffleKey, _, _]] = {
        Map(Result1 -> new TestAggregation(true))
      }

      override def fragments(broadcasts: Map[BroadcastId, Broadcast[_]]): (Fragment[Hoge], Map[BranchKey, OutputFragment[_]]) = {
        val fragment1 = new HogeOutputFragment
        val fragment2 = new HogeOutputFragment
        (new EdgeFragment[Hoge](Array(fragment1, fragment2)) {
          override def newDataModel(): Hoge = new Hoge()
        }, Map(
          Result1 -> fragment1,
          Result2 -> fragment2))
      }

      @transient var sk: ShuffleKey = _

      def shuffleKey = {
        if (sk == null) {
          sk = new ShuffleKey(Seq(new IntOption()), Seq.empty)
        }
        sk
      }

      override def shuffleKey(branch: BranchKey, value: Any): ShuffleKey = {
        branch match {
          case Result1 =>
            val shuffleKey = new ShuffleKey(Seq(new IntOption()), Seq.empty)
            shuffleKey.grouping(0).asInstanceOf[IntOption].copyFrom(value.asInstanceOf[Hoge].id)
            shuffleKey
          case _ =>
            shuffleKey.grouping(0).asInstanceOf[IntOption].copyFrom(value.asInstanceOf[Hoge].id)
            shuffleKey
        }
      }
    }
  }
}
