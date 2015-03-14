package com.asakusafw.spark.compiler
package operator
package user

import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.lang.compiler.model.graph.OperatorOutput
import com.asakusafw.spark.runtime.fragment.Fragment
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._

trait OutputFragments extends ClassBuilder {

  def operatorOutputs: Seq[OperatorOutput]

  def defOutputFields(fieldDef: FieldDef): Unit = {
    operatorOutputs.foreach { output =>
      fieldDef.newFinalField(output.getName, classOf[Fragment[_]].asType,
        new TypeSignatureBuilder()
          .newClassType(classOf[Fragment[_]].asType) {
            _.newTypeArgument(SignatureVisitor.INSTANCEOF, output.getDataType.asType)
          }
          .build())
    }
  }

  def initOutputFields(mb: MethodBuilder, nextLocal: Int): Unit = {
    import mb._
    (nextLocal /: operatorOutputs) {
      case (local, output) =>
        val childVar = `var`(classOf[Fragment[_]].asType, local)
        thisVar.push().putField(output.getName, classOf[Fragment[_]].asType, childVar.push())
        childVar.nextLocal
    }
  }

  def getOutputField(mb: MethodBuilder, output: OperatorOutput): Stack = {
    import mb._
    thisVar.push().getField(output.getName, classOf[Fragment[_]].asType)
  }

  def resetOutputs(mb: MethodBuilder): Unit = {
    import mb._
    operatorOutputs.foreach { output =>
      thisVar.push().getField(output.getName, classOf[Fragment[_]].asType).invokeV("reset")
    }
  }
}
