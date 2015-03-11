package com.asakusafw.spark.runtime

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
    assert(rdds.size > 0)
    val sc = rdds.head.sparkContext
    new ZippedPartitionsRDD(sc, sc.cleanF(f), rdds, preservesPartitioning)
  }

  def confluent[K, V](
    rdds: Seq[RDD[(K, V)]], part: Partitioner, ordering: Option[Ordering[K]]): RDD[(K, V)] = {
    ordering match {
      case Some(ord) =>
        zipPartitions[(K, V)](rdds.map(_.shuffle(part, ordering)), preservesPartitioning = true) {
          iters =>
            val buffs = iters.map(_.asInstanceOf[Iterator[(K, V)]].buffered)
            Iterator.continually {
              ((None: Option[K]) /: buffs) {
                case (opt, iter) if iter.hasNext =>
                  opt.map { key =>
                    ord.min(key, iter.head._1)
                  }.orElse(Some(iter.head._1))
                case (opt, _) => opt
              }
            }.takeWhile(_.isDefined).map(_.get).flatMap { key =>
              buffs.flatMap { iter =>
                Iterator.continually {
                  if (iter.hasNext && ord.equiv(iter.head._1, key)) {
                    Some(iter.next)
                  } else None
                }.takeWhile(_.isDefined).map(_.get)
              }
            }
        }
      case None =>
        zipPartitions[(K, V)](rdds.map(_.shuffle(part, ordering)), preservesPartitioning = true) {
          iters => iters.iterator.flatMap(_.asInstanceOf[Iterator[(K, V)]])
        }
    }
  }

  def smcogroup[K](
    rdds: Seq[(RDD[(K, _)], Option[Ordering[K]])],
    part: Partitioner,
    grouping: Ordering[K]): RDD[(K, Array[Iterable[_]])] = {

    val ord = Option(grouping)
    def shuffle[V](rdd: RDD[(K, V)], o: Option[Ordering[K]]): RDD[(K, V)] =
      rdd.shuffle(part, o.orElse(ord))

    zipPartitions[(K, Array[Iterable[_]])](
      rdds.map((shuffle[Any] _).tupled), preservesPartitioning = true) {
        iters =>
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
