package com.asakusafw.spark.compiler.spi

import java.util.ServiceLoader

import scala.collection.JavaConversions._

import org.objectweb.asm.Type

import com.asakusafw.lang.compiler.api.JobflowProcessor.{ Context => JPContext }
import com.asakusafw.lang.compiler.model.graph._
import com.asakusafw.lang.compiler.model.graph.CoreOperator.CoreOperatorKind
import com.asakusafw.spark.compiler.operator.FragmentClassBuilder

trait CoreOperatorCompiler {

  case class Context(jpContext: JPContext)

  def of: CoreOperatorKind

  def compile(operator: CoreOperator)(implicit context: Context): FragmentClassBuilder
}

object CoreOperatorCompiler {

  private[this] var _operatorCompilers: Option[Map[CoreOperatorKind, CoreOperatorCompiler]] = None

  def apply(classLoader: ClassLoader): Map[CoreOperatorKind, CoreOperatorCompiler] = {
    _operatorCompilers.getOrElse(reload(classLoader))
  }

  def reload(classLoader: ClassLoader): Map[CoreOperatorKind, CoreOperatorCompiler] = {
    val ors = ServiceLoader.load(classOf[CoreOperatorCompiler], classLoader).map {
      resolver => resolver.of -> resolver
    }.toMap
    _operatorCompilers = Some(ors)
    ors
  }
}

trait UserOperatorCompiler {

  case class Context()

  def of: Type

  def compile(operator: UserOperator)(implicit context: Context): FragmentClassBuilder
}

object UserOperatorCompiler {

  private[this] var _operatorCompilers: Option[Map[Type, UserOperatorCompiler]] = None

  def apply(classLoader: ClassLoader): Map[Type, UserOperatorCompiler] = {
    _operatorCompilers.getOrElse(reload(classLoader))
  }

  def reload(classLoader: ClassLoader): Map[Type, UserOperatorCompiler] = {
    val ors = ServiceLoader.load(classOf[UserOperatorCompiler], classLoader).map {
      resolver => resolver.of -> resolver
    }.toMap
    _operatorCompilers = Some(ors)
    ors
  }
}
