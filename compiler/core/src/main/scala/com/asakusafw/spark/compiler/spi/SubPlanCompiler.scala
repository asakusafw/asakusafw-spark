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
    flowId: String,
    jpContext: JPContext)

  def of(operator: Operator, classLoader: ClassLoader): Boolean

  def compile(subplan: SubPlan)(implicit context: Context): Type

  def instantiator: Instantiator
}

object SubPlanCompiler {

  private[this] val _operatorCompilers: mutable.Map[ClassLoader, (Operator => SubPlanCompiler)] =
    mutable.WeakHashMap.empty

  def apply(classLoader: ClassLoader): (Operator => SubPlanCompiler) = {
    _operatorCompilers.getOrElse(classLoader, reload(classLoader))
  }

  def reload(classLoader: ClassLoader): (Operator => SubPlanCompiler) = {
    val compilers = ServiceLoader.load(classOf[SubPlanCompiler], classLoader)
    val ors: PartialFunction[Operator, SubPlanCompiler] =
      compilers.map { compiler =>
        {
          case operator: Operator if compiler.of(operator, classLoader) => compiler
        }: PartialFunction[Operator, SubPlanCompiler]
      }.reduce(_ orElse _)
    _operatorCompilers(classLoader) = ors
    ors
  }
}
