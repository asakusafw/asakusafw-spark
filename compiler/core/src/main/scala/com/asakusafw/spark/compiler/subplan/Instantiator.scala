package com.asakusafw.spark.compiler
package subplan

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable

import org.objectweb.asm.Type

import com.asakusafw.lang.compiler.api.JobflowProcessor.{ Context => JPContext }
import com.asakusafw.lang.compiler.planning._
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._

trait Instantiator {

  case class Context(
    mb: MethodBuilder,
    scVar: Var,
    rddVars: mutable.Map[Long, Var],
    nextLocal: AtomicInteger,
    flowId: String,
    jpContext: JPContext)

  def newInstance(
    subplanType: Type,
    subplan: SubPlan)(implicit context: Context): Var
}
