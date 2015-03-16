package com.asakusafw.spark.runtime
package driver

import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner

import java.io.DataInput
import java.io.DataOutput

import scala.reflect.ClassTag

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._

import com.asakusafw.runtime.model.DataModel
import com.asakusafw.runtime.value.IntOption
import com.asakusafw.spark.runtime.fragment._
import com.asakusafw.spark.runtime.orderings._

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

    val driver = new TestAggregateDriver(sc, hoges, part, aggregation)

    val outputs = driver.execute()
    assert(outputs("result").map {
      case (id: IntOption, hoge: Hoge) => (id.get, hoge.price.get)
    }.collect.toSeq.sortBy(_._1) ===
      Seq((0, (0 until 10 by 2).map(_ * 100).sum), (1, (1 until 10 by 2).map(_ * 100).sum)))
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
    @transient prev: RDD[(IntOption, Hoge)],
    @transient part: Partitioner,
    val aggregation: Aggregation[IntOption, Hoge, Hoge])
      extends AggregateDriver[IntOption, Hoge, Hoge, String](sc, prev, part) {

    override def branchKeys: Set[String] = Set("result")

    override def partitioners: Map[String, Partitioner] = Map.empty

    override def orderings[K]: Map[String, Ordering[K]] = Map.empty

    override def fragments[U <: DataModel[U]]: (Fragment[Hoge], Map[String, OutputFragment[String, _, _, U]]) = {
      val fragment = new HogeOutputFragment("result", this)
      val outputs = Map("result" -> fragment)
      (fragment, outputs.asInstanceOf[Map[String, OutputFragment[String, _, _, U]]])
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

  class HogeOutputFragment(
    branch: String,
    prepareKey: PrepareKey[String])
      extends OneToOneOutputFragment[String, Hoge, Hoge](branch, prepareKey) {
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
