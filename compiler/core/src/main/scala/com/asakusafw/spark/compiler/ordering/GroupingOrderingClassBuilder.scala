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

class GroupingOrderingClassBuilder(flowId: String, groupingTypes: Seq[Type])
    extends ClassBuilder(
      Type.getType(s"L${GeneratedClassPackageInternalName}/${flowId}/ordering/GroupingOrdering$$${GroupingOrderingClassBuilder.nextId};"),
      new ClassSignatureBuilder()
        .newSuperclass {
          _.newClassType(classOf[AbstractOrdering[_]].asType) {
            _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[ShuffleKey].asType)
          }
        }
        .build(),
      classOf[AbstractOrdering[_]].asType) {

  override def defMethods(methodDef: MethodDef): Unit = {
    super.defMethods(methodDef)

    methodDef.newMethod("compare", Type.INT_TYPE, Seq(classOf[AnyRef].asType, classOf[AnyRef].asType)) { mb =>
      import mb._
      val xVar = `var`(classOf[AnyRef].asType, thisVar.nextLocal)
      val yVar = `var`(classOf[AnyRef].asType, xVar.nextLocal)
      `return`(
        thisVar.push().invokeV("compare", Type.INT_TYPE,
          xVar.push().cast(classOf[ShuffleKey].asType), yVar.push().cast(classOf[ShuffleKey].asType)))
    }

    methodDef.newMethod("compare", Type.INT_TYPE, Seq(classOf[ShuffleKey].asType, classOf[ShuffleKey].asType)) { mb =>
      import mb._
      val xVar = `var`(classOf[ShuffleKey].asType, thisVar.nextLocal)
      val yVar = `var`(classOf[ShuffleKey].asType, xVar.nextLocal)

      `return`(
        if (groupingTypes.isEmpty) {
          ldc(0)
        } else {
          val xGroupingVar = xVar.push().invokeV("grouping", classOf[Array[Byte]].asType).store(yVar.nextLocal)
          val yGroupingVar = yVar.push().invokeV("grouping", classOf[Array[Byte]].asType).store(xGroupingVar.nextLocal)
          val xOffsetVar = ldc(0).store(yGroupingVar.nextLocal)
          val yOffsetVar = ldc(0).store(xOffsetVar.nextLocal)
          val xLengthVar = xGroupingVar.push().arraylength().store(yOffsetVar.nextLocal)
          val yLengthVar = yGroupingVar.push().arraylength().store(xLengthVar.nextLocal)
          def compare(t: Type, tail: Seq[Type]): Stack = {
            val cmp = invokeStatic(
              t,
              "compareBytes",
              Type.INT_TYPE,
              xGroupingVar.push(), xOffsetVar.push(), xLengthVar.push().subtract(xOffsetVar.push()),
              yGroupingVar.push(), yOffsetVar.push(), yLengthVar.push().subtract(yOffsetVar.push()))
            if (tail.isEmpty) {
              cmp
            } else {
              cmp.dup().ifEq0({
                cmp.pop()
                xOffsetVar.push().add(
                  invokeStatic(t, "getBytesLength", Type.INT_TYPE,
                    xGroupingVar.push(), xOffsetVar.push(), xLengthVar.push().subtract(xOffsetVar.push())))
                  .store(xOffsetVar.local)
                yOffsetVar.push().add(
                  invokeStatic(t, "getBytesLength", Type.INT_TYPE,
                    yGroupingVar.push(), yOffsetVar.push(), yLengthVar.push().subtract(yOffsetVar.push())))
                  .store(yOffsetVar.local)
                compare(tail.head, tail.tail)
              }, cmp)
            }
          }
          compare(groupingTypes.head, groupingTypes.tail)
        })
    }
  }
}

object GroupingOrderingClassBuilder {

  private[this] val curId: AtomicLong = new AtomicLong(0L)

  def nextId: Long = curId.getAndIncrement

  private[this] val cache: mutable.Map[JPContext, mutable.Map[(String, Seq[Type]), Type]] =
    mutable.WeakHashMap.empty

  def getOrCompile(
    flowId: String,
    groupingTypes: Seq[Type],
    jpContext: JPContext): Type = {
    cache.getOrElseUpdate(jpContext, mutable.Map.empty).getOrElseUpdate(
      (flowId, groupingTypes), {
        jpContext.addClass(new GroupingOrderingClassBuilder(flowId, groupingTypes))
      })
  }
}
