package com.asakusafw.spark.compiler.spi

import java.util.ServiceLoader

import scala.collection.mutable
import scala.collection.JavaConversions._

import org.objectweb.asm.Type

import com.asakusafw.lang.compiler.api.reference.ExternalInputReference
import com.asakusafw.lang.compiler.api.JobflowProcessor.{ Context => JPContext }
import com.asakusafw.lang.compiler.planning.SubPlan
import com.asakusafw.spark.compiler.planning.SubPlanInfo
import com.asakusafw.spark.compiler.subplan._

trait SubPlanCompiler {

  type Context = SubPlanCompiler.Context

  def of: SubPlanInfo.DriverType

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

  def apply(driverType: SubPlanInfo.DriverType)(implicit context: Context): SubPlanCompiler = {
    apply(context.jpContext.getClassLoader)(driverType)
  }

  def get(driverType: SubPlanInfo.DriverType)(implicit context: Context): Option[SubPlanCompiler] = {
    apply(context.jpContext.getClassLoader).get(driverType)
  }

  def support(driverType: SubPlanInfo.DriverType)(implicit context: Context): Boolean = {
    get(driverType).isDefined
  }

  private[this] val subplanCompilers: mutable.Map[ClassLoader, Map[SubPlanInfo.DriverType, SubPlanCompiler]] =
    mutable.WeakHashMap.empty

  private[this] def apply(classLoader: ClassLoader): Map[SubPlanInfo.DriverType, SubPlanCompiler] = {
    subplanCompilers.getOrElse(classLoader, reload(classLoader))
  }

  private[this] def reload(classLoader: ClassLoader): Map[SubPlanInfo.DriverType, SubPlanCompiler] = {
    val ors = ServiceLoader.load(classOf[SubPlanCompiler], classLoader).map {
      resolver => resolver.of -> resolver
    }.toMap[SubPlanInfo.DriverType, SubPlanCompiler]
    subplanCompilers(classLoader) = ors
    ors
  }
}
