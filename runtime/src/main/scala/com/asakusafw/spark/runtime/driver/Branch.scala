package com.asakusafw.spark.runtime.driver

import org.apache.spark.Partitioner

import com.asakusafw.runtime.model.DataModel

trait Branch[B] {

  def branchKeys: Set[B]

  def partitioners: Map[B, Partitioner]

  def orderings[K]: Map[B, Ordering[K]]

  def shuffleKey[U](branch: B, value: DataModel[_]): U
}
