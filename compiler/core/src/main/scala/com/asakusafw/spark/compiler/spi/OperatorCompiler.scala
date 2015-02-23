package com.asakusafw.spark.compiler.spi

import java.util.ServiceLoader

import scala.collection.mutable
import scala.collection.JavaConversions._

import org.objectweb.asm.Type

import com.asakusafw.lang.compiler.api.JobflowProcessor.{ Context => JPContext }
import com.asakusafw.lang.compiler.model.graph._
import com.asakusafw.lang.compiler.model.graph.CoreOperator.CoreOperatorKind
import com.asakusafw.spark.tools.asm.ClassBuilder

trait CoreOperatorCompiler {

  case class Context(jpContext: JPContext)

  def of: CoreOperatorKind

  def compile(operator: CoreOperator)(implicit context: Context): ClassBuilder
}

object CoreOperatorCompiler {

  private[this] val _operatorCompilers: mutable.Map[ClassLoader, Map[CoreOperatorKind, CoreOperatorCompiler]] =
    mutable.WeakHashMap.empty

  def apply(classLoader: ClassLoader): Map[CoreOperatorKind, CoreOperatorCompiler] = {
    _operatorCompilers.getOrElse(classLoader, reload(classLoader))
  }

  def reload(classLoader: ClassLoader): Map[CoreOperatorKind, CoreOperatorCompiler] = {
    val ors = ServiceLoader.load(classOf[CoreOperatorCompiler], classLoader).map {
      resolver => resolver.of -> resolver
    }.toMap
    _operatorCompilers(classLoader) = ors
    ors
  }
}

trait UserOperatorCompiler {

  case class Context(jpContext: JPContext)

  def of: Class[_]

  def compile(operator: UserOperator)(implicit context: Context): ClassBuilder
}

object UserOperatorCompiler {

  private[this] val _operatorCompilers: mutable.Map[ClassLoader, Map[Class[_], UserOperatorCompiler]] =
    mutable.WeakHashMap.empty

  def apply(classLoader: ClassLoader): Map[Class[_], UserOperatorCompiler] = {
    _operatorCompilers.getOrElse(classLoader, reload(classLoader))
  }

  def reload(classLoader: ClassLoader): Map[Class[_], UserOperatorCompiler] = {
    val ors = ServiceLoader.load(classOf[UserOperatorCompiler], classLoader).map {
      resolver => resolver.of -> resolver
    }.toMap[Class[_], UserOperatorCompiler]
    _operatorCompilers(classLoader) = ors
    ors
  }
}
