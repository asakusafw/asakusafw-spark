package com.asakusafw.spark.runtime.driver

import org.apache.spark.Partitioner

trait Branch[B] extends PrepareKey[B] {

  def branchKeys: Set[B]

  def partitioners: Map[B, Partitioner]

  def orderings[K]: Map[B, Ordering[K]]
}
