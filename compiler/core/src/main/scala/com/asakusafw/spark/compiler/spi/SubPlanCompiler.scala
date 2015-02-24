package com.asakusafw.spark.compiler.spi

import java.util.ServiceLoader

import scala.collection.mutable
import scala.collection.JavaConversions._

import org.objectweb.asm.Type

import com.asakusafw.lang.compiler.api.JobflowProcessor.{ Context => JPContext }
import com.asakusafw.lang.compiler.model.graph.Operator
import com.asakusafw.lang.compiler.planning.SubPlan
import com.asakusafw.spark.compiler.subplan._
import com.asakusafw.spark.tools.asm.ClassBuilder

trait SubPlanCompiler {

  case class Context(
    jpContext: JPContext,
    fragments: mutable.ArrayBuffer[ClassBuilder] = mutable.ArrayBuffer.empty)

  def of: SubPlanType

  def compile(subplan: SubPlan)(implicit context: Context): ClassBuilder
}

object SubPlanCompiler {

  private[this] val _operatorCompilers: mutable.Map[ClassLoader, Map[SubPlanType, SubPlanCompiler]] =
    mutable.WeakHashMap.empty

  def apply(classLoader: ClassLoader): Map[SubPlanType, SubPlanCompiler] = {
    _operatorCompilers.getOrElse(classLoader, reload(classLoader))
  }

  def reload(classLoader: ClassLoader): Map[SubPlanType, SubPlanCompiler] = {
    val ors = ServiceLoader.load(classOf[SubPlanCompiler], classLoader).map {
      resolver => resolver.of -> resolver
    }.toMap
    _operatorCompilers(classLoader) = ors
    ors
  }
}
