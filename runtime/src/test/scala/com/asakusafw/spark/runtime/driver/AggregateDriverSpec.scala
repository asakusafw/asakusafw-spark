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
import com.asakusafw.spark.runtime.fragment._

@RunWith(classOf[JUnitRunner])
class AggregateDriverSpecTest extends AggregateDriverSpec

class AggregateDriverSpec extends FlatSpec with SparkSugar {

  import AggregateDriverSpec._

  behavior of classOf[AggregateDriver[_, _, _, _]].getSimpleName

  it should "aggregate" in {
    val hoges = sc.parallelize(0 until 10).map { i =>
      val hoge = new Hoge()
      hoge.id.modify(i % 2)
      hoge.price.modify(i * 100)
      (hoge.id, hoge)
    }

    val part = new GroupingPartitioner(2)
    val aggregation = new TestAggregation()

    val driver = new TestAggregateDriver(sc, hadoopConf, hoges, part, aggregation)

    val outputs = driver.execute()
    assert(outputs("result").map {
      case (id: IntOption, hoge: Hoge) => (id.get, hoge.price.get)
    }.collect.toSeq.sortBy(_._1) ===
      Seq((0, (0 until 10 by 2).map(_ * 100).sum), (1, (1 until 10 by 2).map(_ * 100).sum)))
  }

  it should "aggregate partially" in {
    val hoges = sc.parallelize(0 until 10, 2).map { i =>
      val hoge = new Hoge()
      hoge.id.modify(i % 2)
      hoge.price.modify(i * 100)
      (hoge.id, hoge)
    }

    val driver = new TestPartialAggregationMapDriver(sc, hadoopConf, hoges)

    val outputs = driver.execute()
    assert(outputs("result").map {
      case (id: IntOption, hoge: Hoge) => (id.get, hoge.price.get)
    }.collect.toSeq.sortBy(_._1) ===
      Seq(
        (0, (0 until 10).filter(_ % 2 == 0).filter(_ < 5).map(_ * 100).sum),
        (0, (0 until 10).filter(_ % 2 == 0).filterNot(_ < 5).map(_ * 100).sum),
        (1, (0 until 10).filter(_ % 2 == 1).filter(_ < 5).map(_ * 100).sum),
        (1, (0 until 10).filter(_ % 2 == 1).filterNot(_ < 5).map(_ * 100).sum)))
  }
}

object AggregateDriverSpec {

  class TestAggregation extends Aggregation[IntOption, Hoge, Hoge] {

    override def mapSideCombine: Boolean = true

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
    @transient prev: RDD[(IntOption, Hoge)],
    @transient part: Partitioner,
    val aggregation: Aggregation[IntOption, Hoge, Hoge])
      extends AggregateDriver[IntOption, Hoge, Hoge, String](sc, hadoopConf, Seq(prev), part) {

    override def name = "TestAggregation"

    override def branchKeys: Set[String] = Set("result")

    override def partitioners: Map[String, Partitioner] = Map.empty

    override def orderings[K]: Map[String, Ordering[K]] = Map.empty

    override def aggregations: Map[String, Aggregation[_, _, _]] = Map.empty

    override def fragments[U <: DataModel[U]]: (Fragment[Hoge], Map[String, OutputFragment[U]]) = {
      val fragment = new HogeOutputFragment
      val outputs = Map("result" -> fragment)
      (fragment, outputs.asInstanceOf[Map[String, OutputFragment[U]]])
    }

    override def shuffleKey[U](branch: String, value: DataModel[_]): U = {
      value.asInstanceOf[Hoge].id.asInstanceOf[U]
    }
  }

  class TestPartialAggregationMapDriver(
    @transient sc: SparkContext,
    @transient hadoopConf: Broadcast[Configuration],
    @transient prev: RDD[(IntOption, Hoge)])
      extends MapDriver[Hoge, String](sc, hadoopConf, Seq(prev.asInstanceOf[RDD[(_, Hoge)]])) {

    override def name = "TestPartialAggregation"

    override def branchKeys: Set[String] = Set("result")

    override def partitioners: Map[String, Partitioner] = {
      Map("result" -> new GroupingPartitioner(2))
    }

    override def orderings[K]: Map[String, Ordering[K]] = Map.empty

    override def aggregations: Map[String, Aggregation[_, _, _]] = {
      Map("result" -> new TestAggregation)
    }

    override def fragments[U <: DataModel[U]]: (Fragment[Hoge], Map[String, OutputFragment[U]]) = {
      val fragment = new HogeOutputFragment
      val outputs = Map("result" -> fragment)
      (fragment, outputs.asInstanceOf[Map[String, OutputFragment[U]]])
    }

    override def shuffleKey[U](branch: String, value: DataModel[_]): U = {
      value.asInstanceOf[Hoge].id.asInstanceOf[U]
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

  class GroupingPartitioner(val numPartitions: Int) extends Partitioner {

    override def getPartition(key: Any): Int = {
      val group = key.asInstanceOf[IntOption]
      val part = group.hashCode % numPartitions
      if (part < 0) part + numPartitions else part
    }
  }
}
