package com.asakusafw.spark.compiler.spi

import java.util.ServiceLoader

import scala.collection.mutable
import scala.collection.JavaConversions._

import org.objectweb.asm.Type

import com.asakusafw.lang.compiler.api.JobflowProcessor.{ Context => JPContext }
import com.asakusafw.lang.compiler.model.graph._
import com.asakusafw.lang.compiler.model.graph.CoreOperator.CoreOperatorKind
import com.asakusafw.spark.compiler.operator.{ OperatorCompiler, OperatorType }
import com.asakusafw.spark.tools.asm.ClassBuilder

trait CoreOperatorCompiler extends OperatorCompiler {

  override def support(operator: Operator)(implicit context: Context): Boolean = {
    operator match {
      case op: CoreOperator => support(op)
      case _                => false
    }
  }

  def support(operator: CoreOperator)(implicit context: Context): Boolean

  override def compile(operator: Operator)(implicit context: Context): Type = {
    operator match {
      case op: CoreOperator => compile(op)
    }
  }

  def compile(operator: CoreOperator)(implicit context: Context): Type
}

object CoreOperatorCompiler {

  private[this] val operatorCompilers: mutable.Map[ClassLoader, Seq[CoreOperatorCompiler]] =
    mutable.WeakHashMap.empty

  def apply(classLoader: ClassLoader): Seq[CoreOperatorCompiler] = {
    operatorCompilers.getOrElse(classLoader, reload(classLoader))
  }

  def reload(classLoader: ClassLoader): Seq[CoreOperatorCompiler] = {
    val ors = ServiceLoader.load(classOf[CoreOperatorCompiler], classLoader).toSeq
    operatorCompilers(classLoader) = ors
    ors
  }
}

trait UserOperatorCompiler extends OperatorCompiler {

  override def support(operator: Operator)(implicit context: Context): Boolean = {
    operator match {
      case op: UserOperator => support(op)
      case _                => false
    }
  }

  def support(operator: UserOperator)(implicit context: Context): Boolean

  override def compile(operator: Operator)(implicit context: Context): Type = {
    operator match {
      case op: UserOperator => compile(op)
    }
  }

  def compile(operator: UserOperator)(implicit context: Context): Type
}

object UserOperatorCompiler {

  private[this] val operatorCompilers: mutable.Map[ClassLoader, Seq[UserOperatorCompiler]] =
    mutable.WeakHashMap.empty

  def apply(classLoader: ClassLoader): Seq[UserOperatorCompiler] = {
    operatorCompilers.getOrElse(classLoader, reload(classLoader))
  }

  def reload(classLoader: ClassLoader): Seq[UserOperatorCompiler] = {
    val ors = ServiceLoader.load(classOf[UserOperatorCompiler], classLoader).toSeq
    operatorCompilers(classLoader) = ors
    ors
  }
}
