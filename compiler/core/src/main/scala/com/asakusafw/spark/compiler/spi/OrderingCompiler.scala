package com.asakusafw.spark.compiler.spi

import java.util.ServiceLoader

import scala.collection.JavaConversions._
import scala.math.BigDecimal

import org.objectweb.asm.Type

import com.asakusafw.runtime.value
import com.asakusafw.spark.runtime.orderings
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._

trait OrderingCompiler {

  def of: Type
  def compare(x: Var, y: Var)(implicit mb: MethodBuilder): Stack
}

object OrderingCompiler {

  private[this] var _orderingCompilers: Option[Map[Type, OrderingCompiler]] = None

  def apply(classLoader: ClassLoader): Map[Type, OrderingCompiler] = {
    _orderingCompilers.getOrElse(reload(classLoader))
  }

  def reload(classLoader: ClassLoader): Map[Type, OrderingCompiler] = {
    val ors = ServiceLoader.load(classOf[OrderingCompiler], classLoader).map {
      resolver => resolver.of -> resolver
    }.toMap
    _orderingCompilers = Some(ors)
    ors
  }
}
