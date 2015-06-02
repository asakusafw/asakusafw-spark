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
            Iterator.continually {
              ((None: Option[K]) /: buffs) {
                case (opt, iter) if iter.hasNext =>
                  opt.map { key =>
                    ord.min(key, iter.head._1)
                  }.orElse(Some(iter.head._1))
                case (opt, _) => opt
              }
            }.takeWhile(_.isDefined).map(_.get).flatMap { key =>
              buffs.iterator.flatMap { iter =>
                Iterator.continually {
                  if (iter.hasNext && ord.equiv(iter.head._1, key)) {
                    Some(iter.next)
                  } else None
                }.takeWhile(_.isDefined).map(_.get)
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
    grouping: Ordering[K]): RDD[(K, Seq[Iterator[_]])] = {
    assert(rdds.size > 0,
      s"The size of RDDs to be smcogrouped should be more than 0: ${rdds.size}")

    val ord = Option(grouping)
    val shuffle: ((RDD[(K, Any)], Option[Ordering[K]])) => RDD[(K, _)] = {
      case (rdd, o) => rdd.shuffle(part, o.orElse(ord))
    }

    zipPartitions(rdds.map(shuffle), preservesPartitioning = true) { iters =>
      val buffs = iters.map(_.asInstanceOf[Iterator[(K, _)]].buffered)
      var values = Seq.empty[Iterator[_]]
      Iterator.continually {
        values.foreach { iter =>
          while (iter.hasNext) iter.next()
        }
        ((None: Option[K]) /: buffs) {
          case (opt, iter) if iter.hasNext =>
            opt.map { key =>
              grouping.min(key, iter.head._1)
            }.orElse(Some(iter.head._1))
          case (opt, _) => opt
        }
      }.takeWhile(_.isDefined).map(_.get).map { key =>
        values = buffs.map { buff =>
          new Iterator[Any] {
            def hasNext = buff.hasNext && grouping.equiv(buff.head._1, key)
            def next() = buff.next()._2
          }
        }
        key -> values
      }
    }
  }
}
