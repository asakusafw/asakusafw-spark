package com.asakusafw.spark.spi

import java.util.ServiceLoader

import scala.collection.JavaConversions._

import org.objectweb.asm.Type

object OrderingCompilers {

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
