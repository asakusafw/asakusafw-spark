package com.asakusafw.spark.compiler.operator
package core

import com.asakusafw.lang.compiler.model.graph.CoreOperator
import com.asakusafw.lang.compiler.model.graph.CoreOperator.CoreOperatorKind
import com.asakusafw.spark.compiler.spi.CoreOperatorCompiler

class ExtendOperatorCompiler extends CoreOperatorCompiler {

  def of: CoreOperatorKind = CoreOperatorKind.EXTEND

  def compile(operator: CoreOperator)(implicit context: Context): FragmentClassBuilder = {
    ???
  }
}
