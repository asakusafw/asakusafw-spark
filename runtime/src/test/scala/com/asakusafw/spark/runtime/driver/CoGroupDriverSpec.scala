package com.asakusafw.spark.runtime
package driver

import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner

import java.io.DataInput
import java.io.DataOutput

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
class CoGroupDriverSpecTest extends CoGroupDriverSpec

class CoGroupDriverSpec extends FlatSpec with SparkSugar {

  import CoGroupDriverSpec._

  behavior of "CoGroupDriver"

  it should "cogroup" in {
    val hogeOrd = implicitly[Ordering[(IntOption, Boolean)]].asInstanceOf[Ordering[Product]]
    val hoges = sc.parallelize(0 until 10).map { i =>
      val hoge = new Hoge()
      hoge.id.modify(i)
      ((hoge.id, hoge.id.get % 3 == 0), hoge)
    }.asInstanceOf[RDD[(Product, Any)]]

    val fooOrd = implicitly[Ordering[(IntOption, Int)]].asInstanceOf[Ordering[Product]]
    val foos = sc.parallelize(0 until 10).flatMap(i => (0 until i).map { j =>
      val foo = new Foo()
      foo.id.modify(10 + j)
      foo.hogeId.modify(i)
      ((foo.hogeId, foo.id.toString.hashCode), foo)
    }).asInstanceOf[RDD[(Product, Any)]]

    val part = new GroupingPartitioner(2)
    val groupingOrd = new GroupingOrdering
    val driver = new TestCoGroupDriver[Product](
      sc, Seq((Seq(hoges), Some(hogeOrd)), (Seq(foos), Some(fooOrd))), part, groupingOrd)

    val outputs = driver.execute()
    outputs.mapValues(_.collect.toSeq).foreach {
      case ("hogeResult", values) =>
        val hogeResults = values.asInstanceOf[Seq[(Hoge, Hoge)]]
        assert(hogeResults.size === 1)
        assert(hogeResults(0)._2.id.get === 1)
      case ("fooResult", values) =>
        val fooResults = values.asInstanceOf[Seq[(Foo, Foo)]]
        assert(fooResults.size === 1)
        assert(fooResults(0)._2.id.get === 10)
        assert(fooResults(0)._2.hogeId.get === 1)
      case ("hogeError", values) =>
        val hogeErrors = values.asInstanceOf[Seq[(Hoge, Hoge)]].sortBy(_._1.id)
        assert(hogeErrors.size === 9)
        assert(hogeErrors(0)._2.id.get === 0)
        for (i <- 2 until 10) {
          assert(hogeErrors(i - 1)._2.id.get === i)
        }
      case ("fooError", values) =>
        val fooErrors = values.asInstanceOf[Seq[(Foo, Foo)]].sortBy(_._1.hogeId)
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

  class TestCoGroupDriver[K <: Product: ClassTag](
    @transient sc: SparkContext,
    @transient inputs: Seq[(Seq[RDD[(K, _)]], Option[Ordering[K]])],
    @transient part: Partitioner,
    groupingOrdering: Ordering[K])
      extends CoGroupDriver[String, K](sc, inputs, part, groupingOrdering) {

    override def name = "TestCoGroup"

    override def branchKeys: Set[String] = {
      Set("hogeResult", "fooResult", "hogeError", "fooError")
    }

    override def partitioners: Map[String, Partitioner] = Map.empty

    override def orderings[K]: Map[String, Ordering[K]] = Map.empty

    override def aggregations: Map[String, Aggregation[_, _, _]] = Map.empty

    override def fragments[U <: DataModel[U]]: (Fragment[Seq[Iterable[_]]], Map[String, OutputFragment[U]]) = {
      val outputs = Map(
        "hogeResult" -> new HogeOutputFragment,
        "fooResult" -> new FooOutputFragment,
        "hogeError" -> new HogeOutputFragment,
        "fooError" -> new FooOutputFragment)
      val fragment = new TestCoGroupFragment(outputs)
      (fragment, outputs.asInstanceOf[Map[String, OutputFragment[U]]])
    }

    override def shuffleKey[U](branch: String, value: DataModel[_]): U = {
      value.asInstanceOf[U]
    }
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
      val group = key.asInstanceOf[Product].productElement(0)
      val part = group.hashCode % numPartitions
      if (part < 0) part + numPartitions else part
    }
  }

  class GroupingOrdering extends Ordering[Product] {

    override def compare(x: Product, y: Product): Int = {
      implicitly[Ordering[IntOption]].compare(
        x.productElement(0).asInstanceOf[IntOption],
        y.productElement(0).asInstanceOf[IntOption])
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
