package com.asakusafw.spark.compiler.operator

import org.objectweb.asm.{ Opcodes, Type }

import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._

trait OperatorField extends ClassBuilder {

  def operatorType: Type

  def defOperatorField(fieldDef: FieldDef): Unit = {
    fieldDef.newField(Opcodes.ACC_PRIVATE | Opcodes.ACC_FINAL, "operator", operatorType)
  }

  def initOperatorField(mb: MethodBuilder): Unit = {
    import mb._
    thisVar.push().putField("operator", operatorType, pushNew0(operatorType))
  }

  def getOperatorField(mb: MethodBuilder): Stack = {
    import mb._
    thisVar.push().getField("operator", operatorType)
  }
}
