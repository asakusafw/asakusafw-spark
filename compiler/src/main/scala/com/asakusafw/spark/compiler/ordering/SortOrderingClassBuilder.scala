/*
 * Copyright 2011-2016 Asakusa Framework Team.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.asakusafw.spark.compiler
package ordering

import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable

import org.objectweb.asm.Type
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.spark.compiler.ordering.SortOrderingClassBuilder._
import com.asakusafw.spark.runtime.rdd.ShuffleKey
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._

class SortOrderingClassBuilder(
  groupingOrderingType: Type,
  orderingTypes: Seq[(Type, Boolean)])(
    implicit context: CompilerContext)
  extends ClassBuilder(
    Type.getType(
      s"L${GeneratedClassPackageInternalName}/${context.flowId}/ordering/SortOrdering$$${nextId};"),
    groupingOrderingType) {

  override def defMethods(methodDef: MethodDef): Unit = {
    super.defMethods(methodDef)

    methodDef.newMethod(
      "compare",
      Type.INT_TYPE,
      Seq(classOf[ShuffleKey].asType, classOf[ShuffleKey].asType)) { implicit mb =>
        val thisVar :: xVar :: yVar :: _ = mb.argVars

        val cmp = thisVar.push()
          .invokeS(groupingOrderingType, "compare", Type.INT_TYPE, xVar.push(), yVar.push())

        `return`(
          cmp.dup().ifEq0({
            cmp.pop()
            val xOrderingVar = xVar.push()
              .invokeV("ordering", classOf[Array[Byte]].asType)
              .store()
            val yOrderingVar = yVar.push()
              .invokeV("ordering", classOf[Array[Byte]].asType)
              .store()
            val xOffsetVar = ldc(0).store()
            val yOffsetVar = ldc(0).store()
            val xLengthVar = xOrderingVar.push().arraylength().store()
            val yLengthVar = yOrderingVar.push().arraylength().store()
            def compare(head: (Type, Boolean), tail: Seq[(Type, Boolean)]): Stack = {
              val (t, asc) = head
              val cmp = invokeStatic(
                t,
                "compareBytes",
                Type.INT_TYPE,
                (if (asc) {
                  Seq(xOrderingVar.push(),
                    xOffsetVar.push(),
                    xLengthVar.push().subtract(xOffsetVar.push()),
                    yOrderingVar.push(),
                    yOffsetVar.push(),
                    yLengthVar.push().subtract(yOffsetVar.push()))
                } else {
                  Seq(yOrderingVar.push(),
                    yOffsetVar.push(),
                    yLengthVar.push().subtract(yOffsetVar.push()),
                    xOrderingVar.push(),
                    xOffsetVar.push(),
                    xLengthVar.push().subtract(xOffsetVar.push()))
                }): _*)
              if (tail.isEmpty) {
                cmp
              } else {
                cmp.dup().ifEq0({
                  cmp.pop()
                  xOffsetVar.push().add(
                    invokeStatic(t, "getBytesLength", Type.INT_TYPE,
                      xOrderingVar.push(),
                      xOffsetVar.push(),
                      xLengthVar.push().subtract(xOffsetVar.push())))
                    .store(xOffsetVar.local)
                  yOffsetVar.push().add(
                    invokeStatic(t, "getBytesLength", Type.INT_TYPE,
                      yOrderingVar.push(),
                      yOffsetVar.push(),
                      yLengthVar.push().subtract(yOffsetVar.push())))
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

  private[this] val curIds: mutable.Map[CompilerContext, AtomicLong] =
    mutable.WeakHashMap.empty

  def nextId(implicit context: CompilerContext): Long =
    curIds.getOrElseUpdate(context, new AtomicLong(0L)).getAndIncrement()

  private[this] val cache: mutable.Map[CompilerContext, mutable.Map[(Type, Seq[(Type, Boolean)]), Type]] = // scalastyle:ignore
    mutable.WeakHashMap.empty

  def getOrCompile(
    groupingTypes: Seq[Type],
    orderingTypes: Seq[(Type, Boolean)])(
      implicit context: CompilerContext): Type = {
    getOrCompile(
      GroupingOrderingClassBuilder.getOrCompile(groupingTypes),
      orderingTypes)
  }

  def getOrCompile(
    groupingOrderingType: Type,
    orderingTypes: Seq[(Type, Boolean)])(
      implicit context: CompilerContext): Type = {
    if (orderingTypes.isEmpty) {
      groupingOrderingType
    } else {
      cache.getOrElseUpdate(context, mutable.Map.empty)
        .getOrElseUpdate(
          (groupingOrderingType, orderingTypes),
          context.addClass(new SortOrderingClassBuilder(groupingOrderingType, orderingTypes)))
    }
  }
}
