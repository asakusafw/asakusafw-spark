package com.asakusafw.spark.compiler
package ordering

import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable
import scala.collection.JavaConversions._
import scala.reflect.{ classTag, ClassTag }

import org.objectweb.asm.Type
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.lang.compiler.api.JobflowProcessor.{ Context => JPContext }
import com.asakusafw.lang.compiler.api.reference._
import com.asakusafw.spark.compiler.spi.OrderingCompiler
import com.asakusafw.spark.runtime.orderings.AbstractOrdering
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._

class OrderingClassBuilder(
  flowId: String,
  properties: Seq[(Type, Boolean)],
  compilers: Map[Type, OrderingCompiler])
    extends ClassBuilder(
      Type.getType(s"Lcom/asakusafw/spark/runtime/ordering/Ordering$$${flowId}$$${OrderingClassBuilder.nextId};"),
      OrderingClassBuilder.signature,
      classOf[AbstractOrdering[Seq[_]]].asType) {

  override def defMethods(methodDef: MethodDef): Unit = {
    methodDef.newMethod("compare", Type.INT_TYPE, Seq(classOf[AnyRef].asType, classOf[AnyRef].asType)) { implicit mb =>
      import mb._
      val xVar = `var`(classOf[AnyRef].asType, thisVar.nextLocal)
      val yVar = `var`(classOf[AnyRef].asType, xVar.nextLocal)
      `return`(
        thisVar.push().invokeV("compare", Type.INT_TYPE,
          xVar.push().cast(classOf[Seq[_]].asType), yVar.push().cast(classOf[Seq[_]].asType)))
    }

    methodDef.newMethod("compare", Type.INT_TYPE, Seq(classOf[Seq[_]].asType, classOf[Seq[_]].asType)) { implicit mb =>
      import mb._
      val xVar = `var`(classOf[Seq[_]].asType, thisVar.nextLocal)
      val yVar = `var`(classOf[Seq[_]].asType, xVar.nextLocal)

      if (properties.isEmpty) {
        `return`(ldc(0))
      } else {
        val xIterVar = xVar.push().invokeI("iterator", classOf[Iterator[_]].asType).store(yVar.nextLocal)
        val yIterVar = yVar.push().invokeI("iterator", classOf[Iterator[_]].asType).store(xIterVar.nextLocal)

        def compare(head: (Type, Boolean), tail: Seq[(Type, Boolean)]): Stack = {
          val (t, asc) = head
          val xProp = xIterVar.push().invokeI("next", classOf[AnyRef].asType).cast(t.boxed)
          val xPropVar = (if (t.isPrimitive) xProp.unbox() else xProp).store(yIterVar.nextLocal)
          val yProp = yIterVar.push().invokeI("next", classOf[AnyRef].asType).cast(t.boxed)
          val yPropVar = (if (t.isPrimitive) yProp.unbox() else yProp).store(xPropVar.nextLocal)
          val cmp = if (asc) {
            compilers(t).compare(xPropVar, yPropVar)
          } else {
            compilers(t).compare(yPropVar, xPropVar)
          }
          if (tail.isEmpty) {
            cmp
          } else {
            cmp.dup().ifNe0(
              cmp,
              {
                cmp.pop()
                compare(tail.head, tail.tail)
              })
          }
        }
        `return`(compare(properties.head, properties.tail))
      }
    }
  }
}

object OrderingClassBuilder {

  private[this] val curId: AtomicLong = new AtomicLong(0L)

  def nextId: Long = curId.getAndIncrement

  def signature: String = {
    new ClassSignatureBuilder()
      .newSuperclass {
        _.newClassType(classOf[AbstractOrdering[_]].asType) {
          _.newTypeArgument(SignatureVisitor.INSTANCEOF) {
            _.newClassType(classOf[Seq[_]].asType) {
              _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[Any].asType)
            }
          }
        }
      }
      .build()
  }

  private[this] val cache: mutable.Map[JPContext, mutable.Map[(String, Seq[(Type, Boolean)]), Type]] =
    mutable.WeakHashMap.empty

  def getOrCompile(
    flowId: String,
    properties: Seq[(Type, Boolean)],
    jpContext: JPContext): Type = {
    cache.getOrElseUpdate(jpContext, mutable.Map.empty).getOrElseUpdate(
      (flowId, properties), {
        val compilers = OrderingCompiler(jpContext.getClassLoader)
        jpContext.addClass(new OrderingClassBuilder(flowId, properties, compilers))
      })
  }
}
