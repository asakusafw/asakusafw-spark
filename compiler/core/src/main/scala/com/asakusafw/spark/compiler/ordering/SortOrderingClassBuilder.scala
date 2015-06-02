package com.asakusafw.spark.compiler
package ordering

import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable

import org.objectweb.asm.Type
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.lang.compiler.api.JobflowProcessor.{ Context => JPContext }
import com.asakusafw.spark.runtime.driver.ShuffleKey
import com.asakusafw.spark.runtime.orderings.AbstractOrdering
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._

class SortOrderingClassBuilder(flowId: String, groupingOrderingType: Type, orderingTypes: Seq[(Type, Boolean)])
    extends ClassBuilder(
      Type.getType(s"L${GeneratedClassPackageInternalName}/${flowId}/ordering/SortOrdering$$${SortOrderingClassBuilder.nextId};"),
      groupingOrderingType) {
  assert(orderingTypes.size > 0)

  override def defMethods(methodDef: MethodDef): Unit = {
    super.defMethods(methodDef)

    methodDef.newMethod("compare", Type.INT_TYPE, Seq(classOf[ShuffleKey].asType, classOf[ShuffleKey].asType)) { mb =>
      import mb._
      val xVar = `var`(classOf[ShuffleKey].asType, thisVar.nextLocal)
      val yVar = `var`(classOf[ShuffleKey].asType, xVar.nextLocal)

      val cmp = thisVar.push().invokeS(groupingOrderingType, "compare", Type.INT_TYPE, xVar.push(), yVar.push())

      `return`(
        cmp.dup().ifEq0({
          cmp.pop()
          val xOrderingVar = xVar.push().invokeV("ordering", classOf[Array[Byte]].asType).store(yVar.nextLocal)
          val yOrderingVar = yVar.push().invokeV("ordering", classOf[Array[Byte]].asType).store(xOrderingVar.nextLocal)
          val xOffsetVar = ldc(0).store(yOrderingVar.nextLocal)
          val yOffsetVar = ldc(0).store(xOffsetVar.nextLocal)
          val xLengthVar = xOrderingVar.push().arraylength().store(yOffsetVar.nextLocal)
          val yLengthVar = yOrderingVar.push().arraylength().store(xLengthVar.nextLocal)
          def compare(head: (Type, Boolean), tail: Seq[(Type, Boolean)]): Stack = {
            val (t, asc) = head
            val cmp = invokeStatic(
              t,
              "compareBytes",
              Type.INT_TYPE,
              (if (asc) {
                Seq(xOrderingVar.push(), xOffsetVar.push(), xLengthVar.push().subtract(xOffsetVar.push()),
                  yOrderingVar.push(), yOffsetVar.push(), yLengthVar.push().subtract(yOffsetVar.push()))
              } else {
                Seq(yOrderingVar.push(), yOffsetVar.push(), yLengthVar.push().subtract(yOffsetVar.push()),
                  xOrderingVar.push(), xOffsetVar.push(), xLengthVar.push().subtract(xOffsetVar.push()))
              }): _*)
            if (tail.isEmpty) {
              cmp
            } else {
              cmp.dup().ifEq0({
                cmp.pop()
                xOffsetVar.push().add(
                  invokeStatic(t, "getBytesLength", Type.INT_TYPE,
                    xOrderingVar.push(), xOffsetVar.push(), xLengthVar.push().subtract(xOffsetVar.push())))
                  .store(xOffsetVar.local)
                yOffsetVar.push().add(
                  invokeStatic(t, "getBytesLength", Type.INT_TYPE,
                    yOrderingVar.push(), yOffsetVar.push(), yLengthVar.push().subtract(yOffsetVar.push())))
                  .store(yOffsetVar.local)
                compare(tail.head, tail.tail)
              }, cmp)
            }
          }
          compare(orderingTypes.head, orderingTypes.tail)
        }, cmp))
    }
  }
}

object SortOrderingClassBuilder {

  private[this] val curId: AtomicLong = new AtomicLong(0L)

  def nextId: Long = curId.getAndIncrement

  private[this] val cache: mutable.Map[JPContext, mutable.Map[(String, Type, Seq[(Type, Boolean)]), Type]] =
    mutable.WeakHashMap.empty

  def getOrCompile(
    flowId: String,
    groupingTypes: Seq[Type],
    orderingTypes: Seq[(Type, Boolean)],
    jpContext: JPContext): Type = {
    getOrCompile(
      flowId,
      GroupingOrderingClassBuilder.getOrCompile(flowId, groupingTypes, jpContext),
      orderingTypes,
      jpContext)
  }

  def getOrCompile(
    flowId: String,
    groupingOrderingType: Type,
    orderingTypes: Seq[(Type, Boolean)],
    jpContext: JPContext): Type = {
    if (orderingTypes.isEmpty) {
      groupingOrderingType
    } else {
      cache.getOrElseUpdate(jpContext, mutable.Map.empty).getOrElseUpdate(
        (flowId, groupingOrderingType, orderingTypes), {
          jpContext.addClass(new SortOrderingClassBuilder(flowId, groupingOrderingType, orderingTypes))
        })
    }
  }
}
