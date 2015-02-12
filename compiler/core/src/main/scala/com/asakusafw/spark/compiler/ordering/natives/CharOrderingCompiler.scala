package com.asakusafw.spark.compiler.ordering.natives

import org.objectweb.asm.Type

import com.asakusafw.spark.compiler.spi.OrderingCompiler
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._

class CharOrderingCompiler extends OrderingCompiler {
  def of: Type = Type.CHAR_TYPE
  def compare(x: Var, y: Var)(implicit mb: MethodBuilder): Stack = {
    import mb._
    assert(x.`type` == of)
    assert(y.`type` == of)
    getStatic(Ordering.Char.getClass.asType, "MODULE$", Ordering.Char.getClass.asType)
      .invokeV("compare", Type.INT_TYPE, x.push(), y.push())
  }
}
