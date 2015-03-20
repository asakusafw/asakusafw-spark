package com.asakusafw.spark.compiler.subplan

import com.asakusafw.lang.compiler.model.graph.Operator
import com.asakusafw.spark.tools.asm._

trait DriverName extends ClassBuilder {

  def dominantOperator: Operator

  def defName(methodDef: MethodDef): Unit = {
    methodDef.newMethod("name", classOf[String].asType, Seq.empty) { mb =>
      import mb._
      `return`(ldc(dominantOperator.toString))
    }
  }
}
