package com.asakusafw.spark.compiler
package operator
package user
package join

import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._

trait BroadcastJoin extends JoinOperatorFragmentClassBuilder {

  override def defAddMethod(mb: MethodBuilder, dataModelVar: Var): Unit = {
    import mb._
    ???
  }
}
