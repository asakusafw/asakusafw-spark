package com.asakusafw.spark.compiler
package ordering

import java.util.concurrent.atomic.AtomicLong

import scala.collection.JavaConversions._
import scala.reflect.{ classTag, ClassTag }

import org.objectweb.asm.Type
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.lang.compiler.api.reference._
import com.asakusafw.spark.compiler.spi.OrderingCompiler
import com.asakusafw.spark.runtime.orderings.AbstractOrdering
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._

class OrderingClassBuilder[T] private (
  ownerType: Type,
  signature: String,
  properties: Seq[MethodDesc],
  compilers: Map[Type, OrderingCompiler])
    extends ClassBuilder(
      Type.getType(s"L${ownerType.getInternalName}$$Ordering$$${OrderingClassBuilder.nextId};"),
      signature,
      classOf[AbstractOrdering[T]].asType) {

  assert(properties.forall(_._2.getSort == Type.METHOD))

  override def defMethods(methodDef: MethodDef): Unit = {
    methodDef.newMethod("compare", Type.INT_TYPE, Seq(classOf[AnyRef].asType, classOf[AnyRef].asType)) { implicit mb =>
      import mb._
      val xVar = `var`(classOf[AnyRef].asType, thisVar.nextLocal)
      val yVar = `var`(classOf[AnyRef].asType, xVar.nextLocal)
      `return`(
        thisVar.push().invokeV("compare", Type.INT_TYPE,
          xVar.push().cast(ownerType), yVar.push().cast(ownerType)))
    }

    methodDef.newMethod("compare", Type.INT_TYPE, Seq(ownerType, ownerType)) { implicit mb =>
      import mb._
      val xVar = `var`(ownerType, thisVar.nextLocal)
      val yVar = `var`(ownerType, xVar.nextLocal)

      def compare(head: MethodDesc, tail: Seq[MethodDesc]): Stack = {
        val (methodName, methodType) = head
        val xPropVar = xVar.push().invokeV(methodName, methodType.getReturnType).store(yVar.nextLocal)
        val yPropVar = yVar.push().invokeV(methodName, methodType.getReturnType).store(xPropVar.nextLocal)
        val cmp = compilers(methodType.getReturnType).compare(xPropVar, yPropVar)
        if (tail.isEmpty) {
          cmp
        } else {
          cmp.dup().ifEq0(
            {
              cmp.pop()
              compare(tail.head, tail.tail)
            },
            cmp)
        }
      }

      if (properties.isEmpty) {
        `return`(ldc(0))
      } else {
        `return`(compare(properties.head, properties.tail))
      }
    }
  }
}

object OrderingClassBuilder {

  private[this] val curId: AtomicLong = new AtomicLong(0L)

  def nextId: Long = curId.getAndIncrement

  def apply[T](
    ownerType: Type,
    properties: Seq[MethodDesc],
    compilers: Map[Type, OrderingCompiler]): OrderingClassBuilder[_] = {

    assert(properties.forall(_._2.getSort == Type.METHOD))

    val signature = new ClassSignatureBuilder()
      .newSuperclass {
        _.newClassType(classOf[AbstractOrdering[_]].asType) {
          _.newTypeArgument(SignatureVisitor.INSTANCEOF) {
            _.newClassType(ownerType)
          }
        }
      }
    new OrderingClassBuilder[T](ownerType, signature.build(), properties, compilers)
  }
}
