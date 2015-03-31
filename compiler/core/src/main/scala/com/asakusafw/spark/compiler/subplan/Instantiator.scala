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
    scVar: Var, // SparkContext
    hadoopConfVar: Var, // Broadcast[Configuration]
    rddsVar: Var, // mutable.Map[Long, RDD[_]]
    nextLocal: AtomicInteger,
    flowId: String,
    jpContext: JPContext)

  def newInstance(
    driverType: Type,
    subplan: SubPlan)(implicit context: Context): Var
}
