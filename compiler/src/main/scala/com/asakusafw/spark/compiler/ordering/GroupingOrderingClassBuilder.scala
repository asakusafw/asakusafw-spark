/*
 * Copyright 2011-2015 Asakusa Framework Team.
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

import com.asakusafw.spark.compiler.ordering.GroupingOrderingClassBuilder._
import com.asakusafw.spark.runtime.orderings.AbstractOrdering
import com.asakusafw.spark.runtime.rdd.ShuffleKey
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._

class GroupingOrderingClassBuilder(
  groupingTypes: Seq[Type])(
    implicit context: CompilerContext)
  extends ClassBuilder(
    Type.getType(
      s"L${GeneratedClassPackageInternalName}/${context.flowId}/ordering/GroupingOrdering$$${nextId};"), // scalastyle:ignore
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

    methodDef.newMethod(
      "compare",
      Type.INT_TYPE,
      Seq(classOf[AnyRef].asType, classOf[AnyRef].asType)) { implicit mb =>
        val thisVar :: xVar :: yVar :: _ = mb.argVars

        `return`(
          thisVar.push()
            .invokeV("compare", Type.INT_TYPE,
              xVar.push().cast(classOf[ShuffleKey].asType),
              yVar.push().cast(classOf[ShuffleKey].asType)))
      }

    methodDef.newMethod(
      "compare",
      Type.INT_TYPE,
      Seq(classOf[ShuffleKey].asType, classOf[ShuffleKey].asType)) { implicit mb =>
        val thisVar :: xVar :: yVar :: _ = mb.argVars

        `return`(
          if (groupingTypes.isEmpty) {
            ldc(0)
          } else {
            val xGroupingVar = xVar.push()
              .invokeV("grouping", classOf[Array[Byte]].asType)
              .store()
            val yGroupingVar = yVar.push()
              .invokeV("grouping", classOf[Array[Byte]].asType)
              .store()
            val xOffsetVar = ldc(0).store()
            val yOffsetVar = ldc(0).store()
            val xLengthVar = xGroupingVar.push().arraylength().store()
            val yLengthVar = yGroupingVar.push().arraylength().store()
            def compare(t: Type, tail: Seq[Type]): Stack = {
              val cmp = invokeStatic(
                t,
                "compareBytes",
                Type.INT_TYPE,
                xGroupingVar.push(),
                xOffsetVar.push(),
                xLengthVar.push().subtract(xOffsetVar.push()),
                yGroupingVar.push(),
                yOffsetVar.push(),
                yLengthVar.push().subtract(yOffsetVar.push()))
              if (tail.isEmpty) {
                cmp
              } else {
                cmp.dup().ifEq0({
                  cmp.pop()
                  xOffsetVar.push().add(
                    invokeStatic(t, "getBytesLength", Type.INT_TYPE,
                      xGroupingVar.push(),
                      xOffsetVar.push(),
                      xLengthVar.push().subtract(xOffsetVar.push())))
                    .store(xOffsetVar.local)
                  yOffsetVar.push().add(
                    invokeStatic(t, "getBytesLength", Type.INT_TYPE,
                      yGroupingVar.push(),
                      yOffsetVar.push(),
                      yLengthVar.push().subtract(yOffsetVar.push())))
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

  private[this] val cache: mutable.Map[CompilerContext, mutable.Map[(String, Seq[Type]), Type]] =
    mutable.WeakHashMap.empty

  def getOrCompile(
    groupingTypes: Seq[Type])(
      implicit context: CompilerContext): Type = {
    cache.getOrElseUpdate(context, mutable.Map.empty).getOrElseUpdate(
      (context.flowId, groupingTypes), {
        context.addClass(new GroupingOrderingClassBuilder(groupingTypes))
      })
  }
}
