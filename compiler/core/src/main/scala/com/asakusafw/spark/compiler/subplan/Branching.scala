package com.asakusafw.spark.compiler
package subplan

import org.objectweb.asm.Type

import com.asakusafw.lang.compiler.planning.SubPlan
import com.asakusafw.spark.tools.asm._

trait Branching
  extends ClassBuilder
  with BranchKeysField
  with PartitionersField
  with OrderingsField
  with AggregationsField
  with PreparingKey
  with Serializing
