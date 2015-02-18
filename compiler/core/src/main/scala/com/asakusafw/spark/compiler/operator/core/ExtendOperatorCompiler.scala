package com.asakusafw.spark.compiler.operator
package core

import com.asakusafw.lang.compiler.model.graph.CoreOperator
import com.asakusafw.lang.compiler.model.graph.CoreOperator.CoreOperatorKind
import com.asakusafw.spark.compiler.spi.CoreOperatorCompiler

class ExtendOperatorCompiler extends CoreOperatorCompiler {

  override def of: CoreOperatorKind = CoreOperatorKind.EXTEND

  override def compile(operator: CoreOperator)(implicit context: Context): FragmentClassBuilder = {
    ???
  }
}
