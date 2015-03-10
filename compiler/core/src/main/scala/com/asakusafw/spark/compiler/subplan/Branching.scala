package com.asakusafw.spark.compiler
package subplan

import scala.collection.mutable
import scala.collection.JavaConversions._
import scala.reflect.NameTransformer

import org.objectweb.asm.Type
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.lang.compiler.model.graph.MarkerOperator
import com.asakusafw.lang.compiler.planning.spark.PartitioningParameters
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.spark.tools.asm._

trait Branching
    extends ClassBuilder
    with PreparingKey
    with BranchKeysField
    with PartitionersField
    with OrderingsField {

  def outputMarkers: Seq[MarkerOperator]

  override def defFields(fieldDef: FieldDef): Unit = {
    defBranchKeysField(fieldDef)
    defPartitionersField(fieldDef)
    defOrderingsField(fieldDef)
  }

  def initFields(mb: MethodBuilder): Unit = {
    initBranchKeysField(mb)
    initPartitionersField(mb)
    initOrderingsField(mb)
  }

  override def defMethods(methodDef: MethodDef): Unit = {
    super.defMethods(methodDef)

    defBranchKeys(methodDef)
    defPartitioners(methodDef)
    defOrderings(methodDef)
  }
}
