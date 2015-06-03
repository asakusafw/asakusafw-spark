package com.asakusafw.spark.compiler.spi

import java.util.ServiceLoader

import scala.collection.mutable
import scala.collection.JavaConversions._

import org.objectweb.asm.Type

import com.asakusafw.lang.compiler.api.reference.ExternalInputReference
import com.asakusafw.lang.compiler.api.JobflowProcessor.{ Context => JPContext }
import com.asakusafw.lang.compiler.model.graph.Operator
import com.asakusafw.lang.compiler.planning.SubPlan
import com.asakusafw.spark.compiler.subplan._
import com.asakusafw.spark.tools.asm.ClassBuilder

trait SubPlanCompiler {

  type Context = SubPlanCompiler.Context

  def support(operator: Operator)(implicit context: Context): Boolean

  def compile(subplan: SubPlan)(implicit context: Context): Type

  def instantiator: Instantiator
}

object SubPlanCompiler {

  case class Context(
    flowId: String,
    jpContext: JPContext,
    externalInputs: mutable.Map[String, ExternalInputReference],
    branchKeys: BranchKeys,
    broadcastIds: BroadcastIds)

  private[this] val operatorCompilers: mutable.Map[ClassLoader, Seq[SubPlanCompiler]] =
    mutable.WeakHashMap.empty

  def apply(classLoader: ClassLoader): Seq[SubPlanCompiler] = {
    operatorCompilers.getOrElse(classLoader, reload(classLoader))
  }

  def reload(classLoader: ClassLoader): Seq[SubPlanCompiler] = {
    val ors = ServiceLoader.load(classOf[SubPlanCompiler], classLoader).toSeq
    operatorCompilers(classLoader) = ors
    ors
  }
}
