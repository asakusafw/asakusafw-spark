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
import com.asakusafw.runtime.value.IntOption
import com.asakusafw.spark.runtime.aggregation.Aggregation
import com.asakusafw.spark.runtime.fragment._

@RunWith(classOf[JUnitRunner])
class AggregateDriverSpecTest extends AggregateDriverSpec

class AggregateDriverSpec extends FlatSpec with SparkSugar {

  import AggregateDriverSpec._

  behavior of classOf[AggregateDriver[_, _]].getSimpleName

  it should "aggregate with map-side combine" in {
    val hoges = sc.parallelize(0 until 10).map { i =>
      val hoge = new Hoge()
      hoge.id.modify(i % 2)
      hoge.price.modify(i * 100)
      (new ShuffleKey(Seq(hoge.id), Seq(hoge.price)) {}, hoge)
    }

    val part = new HashPartitioner(2)
    val aggregation = new TestAggregation(true)

    val driver = new TestAggregateDriver(sc, hadoopConf, hoges, Seq(false), part, aggregation)

    val outputs = driver.execute()
    assert(outputs(Result).map {
      case (id: ShuffleKey, hoge: Hoge) => (id.grouping(0).asInstanceOf[IntOption].get, hoge.price.get)
    }.collect.toSeq.sortBy(_._1) ===
      Seq((0, (0 until 10 by 2).map(_ * 100).sum), (1, (1 until 10 by 2).map(_ * 100).sum)))
  }

  it should "aggregate without map-side combine" in {
    val hoges = sc.parallelize(0 until 10).map { i =>
      val hoge = new Hoge()
      hoge.id.modify(i % 2)
      hoge.price.modify(i * 100)
      (new ShuffleKey(Seq(hoge.id), Seq(hoge.price)) {}, hoge)
    }

    val part = new HashPartitioner(2)
    val aggregation = new TestAggregation(false)

    val driver = new TestAggregateDriver(sc, hadoopConf, hoges, Seq(false), part, aggregation)

    val outputs = driver.execute()
    assert(outputs(Result).map {
      case (id: ShuffleKey, hoge: Hoge) => (id.grouping(0).asInstanceOf[IntOption].get, hoge.price.get)
    }.collect.toSeq.sortBy(_._1) ===
      Seq((0, (0 until 10 by 2).map(_ * 100).sum), (1, (1 until 10 by 2).map(_ * 100).sum)))
  }

  it should "aggregate partially" in {
    val hoges = sc.parallelize(0 until 10, 2).map { i =>
      val hoge = new Hoge()
      hoge.id.modify(i % 2)
      hoge.price.modify(i * 100)
      (new ShuffleKey(Seq(hoge.id), Seq(hoge.price)) {}, hoge)
    }

    val driver = new TestPartialAggregationMapDriver(sc, hadoopConf, hoges)

    val outputs = driver.execute()
    assert(outputs(Result).map {
      case (id: ShuffleKey, hoge: Hoge) => (id.grouping(0).asInstanceOf[IntOption].get, hoge.price.get)
    }.collect.toSeq.sortBy(_._1) ===
      Seq(
        (0, (0 until 10).filter(_ % 2 == 0).filter(_ < 5).map(_ * 100).sum),
        (0, (0 until 10).filter(_ % 2 == 0).filterNot(_ < 5).map(_ * 100).sum),
        (1, (0 until 10).filter(_ % 2 == 1).filter(_ < 5).map(_ * 100).sum),
        (1, (0 until 10).filter(_ % 2 == 1).filterNot(_ < 5).map(_ * 100).sum)))
  }
}

object AggregateDriverSpec {

  val Result = BranchKey(0)

  class TestAggregation(val mapSideCombine: Boolean)
      extends Aggregation[ShuffleKey, Hoge, Hoge] {

    override def createCombiner(value: Hoge): Hoge = value

    override def mergeValue(combiner: Hoge, value: Hoge): Hoge = {
      combiner.price.add(value.price)
      combiner
    }

    override def mergeCombiners(comb1: Hoge, comb2: Hoge): Hoge = {
      comb1.price.add(comb2.price)
      comb1
    }
  }

  class TestAggregateDriver(
    @transient sc: SparkContext,
    @transient hadoopConf: Broadcast[Configuration],
    @transient prev: RDD[(ShuffleKey, Hoge)],
    @transient directions: Seq[Boolean],
    @transient part: Partitioner,
    val aggregation: Aggregation[ShuffleKey, Hoge, Hoge])
      extends AggregateDriver[Hoge, Hoge](sc, hadoopConf, Map.empty, Seq(prev), directions, part) {

    override def name = "TestAggregation"

    override def branchKeys: Set[BranchKey] = Set(Result)

    override def partitioners: Map[BranchKey, Partitioner] = Map.empty

    override def orderings: Map[BranchKey, Ordering[ShuffleKey]] = Map.empty

    override def aggregations: Map[BranchKey, Aggregation[ShuffleKey, _, _]] = Map.empty

    override def fragments[U <: DataModel[U]]: (Fragment[Hoge], Map[BranchKey, OutputFragment[U]]) = {
      val fragment = new HogeOutputFragment
      val outputs = Map(Result -> fragment)
      (fragment, outputs.asInstanceOf[Map[BranchKey, OutputFragment[U]]])
    }

    override def shuffleKey(branch: BranchKey, value: Any): ShuffleKey = {
      new ShuffleKey(Seq(value.asInstanceOf[Hoge].id), Seq.empty) {}
    }
  }

  class TestPartialAggregationMapDriver(
    @transient sc: SparkContext,
    @transient hadoopConf: Broadcast[Configuration],
    @transient prev: RDD[(ShuffleKey, Hoge)])
      extends MapDriver[Hoge](sc, hadoopConf, Map.empty, Seq(prev)) {

    override def name = "TestPartialAggregation"

    override def branchKeys: Set[BranchKey] = Set(Result)

    override def partitioners: Map[BranchKey, Partitioner] = {
      Map(Result -> new HashPartitioner(2))
    }

    override def orderings: Map[BranchKey, Ordering[ShuffleKey]] = Map.empty

    override def aggregations: Map[BranchKey, Aggregation[ShuffleKey, _, _]] = {
      Map(Result -> new TestAggregation(true))
    }

    override def fragments[U <: DataModel[U]]: (Fragment[Hoge], Map[BranchKey, OutputFragment[U]]) = {
      val fragment = new HogeOutputFragment
      val outputs = Map(Result -> fragment)
      (fragment, outputs.asInstanceOf[Map[BranchKey, OutputFragment[U]]])
    }

    override def shuffleKey(branch: BranchKey, value: Any): ShuffleKey = {
      new ShuffleKey(Seq(value.asInstanceOf[Hoge].id), Seq.empty) {}
    }
  }

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
}
