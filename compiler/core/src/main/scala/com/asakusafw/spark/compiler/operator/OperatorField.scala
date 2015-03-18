package com.asakusafw.spark.compiler.operator

import org.objectweb.asm.{ Opcodes, Type }

import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._

trait OperatorField extends ClassBuilder {

  def operatorType: Type

  def defOperatorField(fieldDef: FieldDef): Unit = {
    fieldDef.newField(Opcodes.ACC_PUBLIC | Opcodes.ACC_TRANSIENT, "operator", operatorType)
  }

  def getOperatorField(mb: MethodBuilder): Stack = {
    import mb._
    thisVar.push().invokeV("getOperator", operatorType)
  }

  def defGetOperator(methodDef: MethodDef): Unit = {
    methodDef.newMethod("getOperator", operatorType, Seq.empty) { mb =>
      import mb._
      thisVar.push().getField("operator", operatorType).unlessNotNull {
        thisVar.push().putField("operator", operatorType, pushNew0(operatorType))
      }
      `return`(thisVar.push().getField("operator", operatorType))
    }
  }
}
