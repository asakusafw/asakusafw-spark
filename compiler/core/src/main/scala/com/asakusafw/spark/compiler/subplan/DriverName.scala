package com.asakusafw.spark.compiler.subplan

import com.asakusafw.lang.compiler.model.graph.Operator
import com.asakusafw.spark.tools.asm._

trait DriverName extends ClassBuilder {

  def dominantOperator: Operator

  override def defMethods(methodDef: MethodDef): Unit = {
    super.defMethods(methodDef)

    methodDef.newMethod("name", classOf[String].asType, Seq.empty) { mb =>
      import mb._
      `return`(ldc(dominantOperator.toString))
    }
  }
}
