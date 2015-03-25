package com.asakusafw.spark.compiler.spi

import java.util.ServiceLoader

import scala.collection.mutable
import scala.collection.JavaConversions._

import org.objectweb.asm.Type

import com.asakusafw.lang.compiler.api.JobflowProcessor.{ Context => JPContext }
import com.asakusafw.lang.compiler.model.graph._

trait AggregationCompiler {

  type Context = AggregationCompiler.Context

  def of: Class[_]
  def compile(operator: UserOperator)(implicit context: Context): Type
}

object AggregationCompiler {

  case class Context(
    flowId: String,
    jpContext: JPContext)

  private def getCompiler(operator: Operator)(implicit context: Context): Option[AggregationCompiler] = {
    operator match {
      case op: UserOperator =>
        apply(context.jpContext.getClassLoader)
          .get(op.getAnnotation.resolve(context.jpContext.getClassLoader).annotationType)
      case _ => None
    }
  }

  def support(operator: Operator)(implicit context: Context): Boolean = {
    getCompiler(operator).isDefined
  }

  def compile(operator: Operator)(implicit context: Context): Type = {
    getCompiler(operator) match {
      case Some(compiler) => compiler.compile(operator.asInstanceOf[UserOperator])
    }
  }

  private[this] val aggregationCompilers: mutable.Map[ClassLoader, Map[Class[_], AggregationCompiler]] =
    mutable.WeakHashMap.empty

  private[this] def apply(classLoader: ClassLoader): Map[Class[_], AggregationCompiler] = {
    aggregationCompilers.getOrElse(classLoader, reload(classLoader))
  }

  private[this] def reload(classLoader: ClassLoader): Map[Class[_], AggregationCompiler] = {
    val ors = ServiceLoader.load(classOf[AggregationCompiler], classLoader).map {
      resolver => resolver.of -> resolver
    }.toMap[Class[_], AggregationCompiler]
    aggregationCompilers(classLoader) = ors
    ors
  }
}
