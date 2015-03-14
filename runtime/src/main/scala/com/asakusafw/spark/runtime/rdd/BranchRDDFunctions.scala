package com.asakusafw.spark.runtime.rdd

import scala.reflect.ClassTag

import org.apache.spark._
import org.apache.spark.rdd._

import com.asakusafw.spark.runtime.orderings.NoOrdering

class BranchRDDFunctions[T: ClassTag](self: RDD[T]) extends Serializable {

  def branch[B, K, U](
    branchKeys: Set[B],
    f: Iterator[T] => Iterator[((B, K), U)],
    partitioners: Map[B, Partitioner] = Map.empty[B, Partitioner],
    keyOrderings: Map[B, Ordering[K]] = Map.empty[B, Ordering[K]],
    preservesPartitioning: Boolean = false): Map[B, RDD[(K, U)]] = {

    val prepared = self.mapPartitions(f, preservesPartitioning)

    val branchPartitioner = new BranchPartitioner(
      branchKeys,
      partitioners.withDefaultValue(IdentityPartitioner(prepared.partitions.size)))
    val shuffled = new ShuffledRDD[(B, K), U, U](prepared, branchPartitioner)
    if (keyOrderings.nonEmpty) {
      shuffled.setKeyOrdering(
        new BranchKeyOrdering(keyOrderings.withDefaultValue(new NoOrdering[K])))
    }
    val branched = shuffled.map { case ((_, k), u) => (k, u) }
    branchKeys.map {
      case branch =>
        branch -> new BranchedRDD[(K, U)](
          branched,
          partitioners.get(branch).orElse(prepared.partitioner),
          i => {
            val offset = branchPartitioner.offsetOf(branch)
            val numPartitions = branchPartitioner.numPartitionsOf(branch)
            offset <= i && i < offset + numPartitions
          })
    }.toMap
  }
}

private class BranchPartitioner[B](branchKeys: Set[B], partitioners: Map[B, Partitioner])
    extends Partitioner {

  private[this] val branches = branchKeys.toSeq.sortBy(_.hashCode)

  private[this] val offsets = branches.scanLeft(0)(_ + numPartitionsOf(_))

  override def numPartitions: Int = offsets.last

  override def getPartition(key: Any): Int = {
    assert(key.isInstanceOf[(_, _)])
    val (branch, origKey) = key.asInstanceOf[(B, Any)]
    offsetOf(branch) + getPartitionOf(branch, origKey)
  }

  def numPartitionsOf(branch: B): Int = partitioners(branch).numPartitions

  def getPartitionOf(branch: B, key: Any): Int = partitioners(branch).getPartition(key)

  def offsetOf(branch: B): Int = offsets(branches.indexOf(branch))
}

private class BranchKeyOrdering[B, K](orderings: Map[B, Ordering[K]])
    extends Ordering[(B, K)] {

  override def compare(left: (B, K), right: (B, K)): Int = {
    orderings(left._1).compare(left._2, right._2)
  }
}

private class BranchedRDD[T: ClassTag](
  @transient prev: RDD[T],
  @transient part: Option[Partitioner],
  @transient partitionFilterFunc: Int => Boolean)
    extends PartitionPruningRDD[T](prev, partitionFilterFunc) {

  override val partitioner = part
}
