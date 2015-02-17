package com.asakusafw.spark.compiler.ordering.values

import org.objectweb.asm.Type

import com.asakusafw.runtime.value
import com.asakusafw.spark.compiler.spi.OrderingCompiler
import com.asakusafw.spark.runtime.orderings
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._

class FloatOptionOrderingCompiler extends OrderingCompiler {
  def of: Type = classOf[value.FloatOption].asType
  def compare(x: Var, y: Var)(implicit mb: MethodBuilder): Stack = {
    import mb._
    assert(x.`type` == of)
    assert(y.`type` == of)
    getStatic(orderings.FloatOptionOrdering.getClass.asType, "MODULE$", orderings.FloatOptionOrdering.getClass.asType)
      .invokeV("compare", Type.INT_TYPE, x.push(), y.push())
  }
}
