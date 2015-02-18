package com.asakusafw.spark.runtime

import scala.language.implicitConversions
import scala.reflect.ClassTag

import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.ShuffledRDD

import org.apache.spark.backdoor._

package object rdd {

  implicit def rddToBranchRDDFunctions[T: ClassTag](rdd: RDD[T]): BranchRDDFunctions[T] = {
    new BranchRDDFunctions(rdd)
  }

  def zipPartitions[V: ClassTag](
    rdds: Seq[RDD[_]], preservesPartitioning: Boolean = false)(f: (Seq[Iterator[_]] => Iterator[V])): RDD[V] = {
    assert(rdds.size > 1)
    val sc = rdds.head.sparkContext
    new ZippedPartitionsRDD(sc, sc.cleanF(f), rdds, preservesPartitioning)
  }

  def confluent[K: ClassTag, V: ClassTag](
    rdds: Seq[RDD[(K, V)]], part: Partitioner, ordering: Option[Ordering[K]]): RDD[(K, V)] = {
    assert(rdds.size > 0)

    def shuffle(rdd: RDD[(K, V)]): RDD[(K, V)] = {
      if (rdd.partitioner == Some(part)) {
        rdd
      } else {
        new ShuffledRDD[K, V, V](rdd, part).setKeyOrdering(ordering.orNull)
      }
    }

    val zipping = zipPartitions[(K, V)](rdds.map(shuffle), preservesPartitioning = true) _
    ordering match {
      case Some(ord) => zipping {
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
      case None => zipping {
        iters => iters.iterator.flatMap(_.asInstanceOf[Iterator[(K, V)]])
      }
    }
  }
}
