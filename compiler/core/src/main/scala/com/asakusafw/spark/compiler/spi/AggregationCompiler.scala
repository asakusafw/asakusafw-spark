package com.asakusafw.spark.compiler.spi

import java.util.ServiceLoader

import scala.collection.mutable
import scala.collection.JavaConversions._

import org.objectweb.asm.Type

import com.asakusafw.lang.compiler.api.JobflowProcessor.{ Context => JPContext }
import com.asakusafw.lang.compiler.model.graph._

trait AggregationCompiler {

  case class Context(flowId: String, jpContext: JPContext)

  def of: Class[_]
  def compile(operator: UserOperator)(implicit context: Context): Type
}

object AggregationCompiler {

  private[this] val aggregationCompilers: mutable.Map[ClassLoader, Map[Class[_], AggregationCompiler]] =
    mutable.WeakHashMap.empty

  def apply(classLoader: ClassLoader): Map[Class[_], AggregationCompiler] = {
    aggregationCompilers.getOrElse(classLoader, reload(classLoader))
  }

  def reload(classLoader: ClassLoader): Map[Class[_], AggregationCompiler] = {
    val ors = ServiceLoader.load(classOf[AggregationCompiler], classLoader).map {
      resolver => resolver.of -> resolver
    }.toMap[Class[_], AggregationCompiler]
    aggregationCompilers(classLoader) = ors
    ors
  }
}
