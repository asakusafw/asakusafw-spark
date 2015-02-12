package com.asakusafw.spark.compiler.operator
package core

import com.asakusafw.lang.compiler.model.graph.CoreOperator
import com.asakusafw.lang.compiler.model.graph.CoreOperator.CoreOperatorKind
import com.asakusafw.spark.compiler.spi.CoreOperatorCompiler

class ProjectOperatorCompiler extends CoreOperatorCompiler {

  def of: CoreOperatorKind = CoreOperatorKind.PROJECT

  def compile(operator: CoreOperator)(implicit context: Context): FragmentClassBuilder = {
    ???
  }
}
