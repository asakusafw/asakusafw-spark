package com.asakusafw.spark.compiler
package subplan

import org.objectweb.asm.Type

import com.asakusafw.lang.compiler.planning.SubPlan
import com.asakusafw.spark.tools.asm._

trait Branching
    extends ClassBuilder
    with PreparingKey
    with BranchKeysField
    with PartitionersField
    with OrderingsField
    with AggregationsField {

  override def subplanOutputs: Seq[SubPlan.Output]

  override def defFields(fieldDef: FieldDef): Unit = {
    defBranchKeysField(fieldDef)
    defPartitionersField(fieldDef)
    defOrderingsField(fieldDef)
    defAggregationsField(fieldDef)
  }

  def initFields(mb: MethodBuilder): Unit = {
    initBranchKeysField(mb)
    initPartitionersField(mb)
    initOrderingsField(mb)
    initAggregationsField(mb)
  }

  override def defMethods(methodDef: MethodDef): Unit = {
    super.defMethods(methodDef)

    defBranchKeys(methodDef)
    defPartitioners(methodDef)
    defOrderings(methodDef)
    defAggregations(methodDef)
  }
}
