package com.asakusafw.spark.runtime

import scala.collection.mutable
import scala.language.implicitConversions
import scala.reflect.ClassTag

import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.ShuffledRDD

import org.apache.spark.backdoor._
import org.apache.spark.util.collection.backdoor.CompactBuffer

package object rdd {

  implicit def rddToBranchRDDFunctions[T: ClassTag](rdd: RDD[T]): BranchRDDFunctions[T] = {
    new BranchRDDFunctions(rdd)
  }

  implicit class AugmentedPairRDD[K, V](rdd: RDD[(K, V)]) {

    def shuffle(part: Partitioner, ordering: Option[Ordering[K]] = None): RDD[(K, V)] = {
      if (rdd.partitioner == Some(part)) {
        rdd
      } else {
        new ShuffledRDD[K, V, V](rdd, part).setKeyOrdering(ordering.orNull)
      }
    }
  }

  def zipPartitions[V: ClassTag](
    rdds: Seq[RDD[_]], preservesPartitioning: Boolean = false)(f: (Seq[Iterator[_]] => Iterator[V])): RDD[V] = {
    assert(rdds.size > 0,
      s"The size of RDDs to be zipped should be more than 0: ${rdds.size}")
    val sc = rdds.head.sparkContext
    new ZippedPartitionsRDD(sc, sc.clean(f), rdds, preservesPartitioning)
  }

  def confluent[K, V](
    rdds: Seq[RDD[(K, V)]], part: Partitioner, ordering: Option[Ordering[K]]): RDD[(K, V)] = {
    assert(rdds.size > 0,
      s"The size of RDDs to be confluented should be more than 0: ${rdds.size}")
    if (rdds.size > 1) {
      ordering match {
        case Some(ord) =>
          zipPartitions(rdds.map(_.shuffle(part, ordering)), preservesPartitioning = true) { iters =>
            val buffs = iters.filter(_.hasNext).map(_.asInstanceOf[Iterator[(K, V)]].buffered)

            type Iter = BufferedIterator[(K, V)]
            val heap = new mutable.PriorityQueue[Iter]()(
              new Ordering[Iter] {
                override def compare(x: Iter, y: Iter): Int = -ord.compare(x.head._1, y.head._1)
              })
            heap.enqueue(buffs: _*)

            new Iterator[(K, V)] {

              override def hasNext: Boolean = !heap.isEmpty

              override def next(): (K, V) = {
                if (!hasNext) {
                  throw new NoSuchElementException
                }
                val firstBuf = heap.dequeue()
                val firstPair = firstBuf.next()
                if (firstBuf.hasNext) {
                  heap.enqueue(firstBuf)
                }
                firstPair
              }
            }
          }
        case None =>
          zipPartitions(rdds.map(_.shuffle(part, ordering)), preservesPartitioning = true)(
            _.iterator.flatMap(_.asInstanceOf[Iterator[(K, V)]]))
      }
    } else {
      rdds.head.shuffle(part, ordering)
    }
  }

  def smcogroup[K](
    rdds: Seq[(RDD[(K, _)], Option[Ordering[K]])],
    part: Partitioner,
    grouping: Ordering[K]): RDD[(K, Array[Iterable[_]])] = {
    assert(rdds.size > 0,
      s"The size of RDDs to be smcogrouped should be more than 0: ${rdds.size}")

    val ord = Option(grouping)
    val shuffle: ((RDD[(K, Any)], Option[Ordering[K]])) => RDD[(K, _)] = {
      case (rdd, o) => rdd.shuffle(part, o.orElse(ord))
    }

    zipPartitions(rdds.map(shuffle), preservesPartitioning = true) { iters =>
      val buffs = iters.map(_.asInstanceOf[Iterator[(K, _)]].buffered)
      Iterator.continually {
        ((None: Option[K]) /: buffs) {
          case (opt, iter) if iter.hasNext =>
            opt.map { key =>
              grouping.min(key, iter.head._1)
            }.orElse(Some(iter.head._1))
          case (opt, _) => opt
        }
      }.takeWhile(_.isDefined).map(_.get).map { key =>
        val values = Array.fill[Iterable[_]](iters.size)(new CompactBuffer[Any])
        buffs.zipWithIndex.foreach {
          case (iter, i) =>
            while (iter.hasNext && grouping.equiv(iter.head._1, key)) {
              values(i).asInstanceOf[CompactBuffer[Any]] += iter.next._2
            }
        }
        key -> values
      }
    }
  }
}
