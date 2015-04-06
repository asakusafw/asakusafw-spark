package com.asakusafw.spark.compiler.spi

import java.util.ServiceLoader

import scala.collection.mutable
import scala.collection.JavaConversions._

import org.objectweb.asm.Type

import com.asakusafw.lang.compiler.api.JobflowProcessor.{ Context => JPContext }
import com.asakusafw.lang.compiler.model.graph._
import com.asakusafw.spark.tools.asm.ClassBuilder

sealed trait OperatorType

object OperatorType {

  case object MapType extends OperatorType
  case object CoGroupType extends OperatorType
  case object AggregationType extends OperatorType
}

trait OperatorCompiler {

  type Context = OperatorCompiler.Context

  def support(operator: Operator)(implicit context: Context): Boolean

  def operatorType: OperatorType

  def compile(operator: Operator)(implicit context: Context): Type
}

object OperatorCompiler {

  case class Context(
    flowId: String,
    jpContext: JPContext,
    shuffleKeyTypes: mutable.Set[Type])

  private def getCompiler(operator: Operator)(implicit context: Context): Seq[OperatorCompiler] = {
    apply(context.jpContext.getClassLoader).filter(_.support(operator))
  }

  def support(operator: Operator, operatorType: OperatorType)(implicit context: Context): Boolean = {
    getCompiler(operator).exists(_.operatorType == operatorType)
  }

  def compile(operator: Operator, operatorType: OperatorType)(implicit context: Context): Type = {
    val compilers = getCompiler(operator).filter(_.operatorType == operatorType)
    assert(compilers.size != 0,
      s"The compiler supporting operator (${operator}, ${operatorType}) is not found.")
    assert(compilers.size == 1,
      s"The number of compiler supporting operator (${operator}, ${operatorType}) should be 1: ${compilers.size}")
    compilers.head.compile(operator)
  }

  private[this] val operatorCompilers: mutable.Map[ClassLoader, Seq[OperatorCompiler]] =
    mutable.WeakHashMap.empty

  private[this] def apply(classLoader: ClassLoader): Seq[OperatorCompiler] = {
    operatorCompilers.getOrElse(classLoader, reload(classLoader))
  }

  private[this] def reload(classLoader: ClassLoader): Seq[OperatorCompiler] = {
    val ors = ServiceLoader.load(classOf[OperatorCompiler], classLoader).toSeq
    operatorCompilers(classLoader) = ors
    ors
  }
}
