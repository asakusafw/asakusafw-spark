/*
 * Copyright 2011-2017 Asakusa Framework Team.
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
package operator

import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable

import org.objectweb.asm.{ Opcodes, Type }
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.spark.compiler.operator.MapGroupViewClassBuilder._
import com.asakusafw.spark.runtime.fragment.MapGroupView
import com.asakusafw.spark.runtime.io.WritableSerDe
import com.asakusafw.spark.runtime.rdd.ShuffleKey
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._
import com.asakusafw.spark.tools.asm4s._

class MapGroupViewClassBuilder(
  keyElementTypes: Seq[Type])(
    implicit context: CompilerContext)
  extends ClassBuilder(
    Type.getType(
      s"L${GeneratedClassPackageInternalName}/${context.flowId}/fragment/MapGroupView$$${nextId};"),
    new ClassSignatureBuilder()
      .newFormalTypeParameter("V", classOf[AnyRef].asType)
      .newSuperclass {
        _.newClassType(classOf[MapGroupView[_, _]].asType) {
          _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[ShuffleKey].asType)
            .newTypeArgument(SignatureVisitor.INSTANCEOF, "V")
        }
      },
    classOf[MapGroupView[_, _]].asType) {

  override def defConstructors(ctorDef: ConstructorDef): Unit = {
    ctorDef.newInit(
      Seq(classOf[Map[_, _]].asType),
      new MethodSignatureBuilder()
        .newParameterType {
          _.newClassType(classOf[Map[_, _]].asType) {
            _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[ShuffleKey].asType)
              .newTypeArgument(SignatureVisitor.INSTANCEOF) {
                _.newClassType(classOf[Seq[_]].asType) {
                  _.newTypeArgument()
                }
              }
          }
        }
        .newVoidReturnType()) { implicit mb =>
        val thisVar :: mapVar :: _ = mb.argVars
        thisVar.push().invokeInit(superType, mapVar.push())
      }
  }

  override def defMethods(methodDef: MethodDef): Unit = {
    super.defMethods(methodDef)

    methodDef.newMethod(
      Opcodes.ACC_PROTECTED,
      "key",
      classOf[AnyRef].asType,
      Seq(classOf[Seq[AnyRef]].asType),
      new MethodSignatureBuilder()
        .newParameterType {
          _.newClassType(classOf[Seq[_]].asType) {
            _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[AnyRef].asType)
          }
        }
        .newReturnType(classOf[AnyRef].asType)) { implicit mb =>
        val thisVar :: elementsVar :: _ = mb.argVars
        `return`(
          thisVar.push().invokeV("key", classOf[ShuffleKey].asType, elementsVar.push()))
      }

    methodDef.newMethod(
      Opcodes.ACC_PROTECTED,
      "key",
      classOf[ShuffleKey].asType,
      Seq(classOf[Seq[AnyRef]].asType),
      new MethodSignatureBuilder()
        .newParameterType {
          _.newClassType(classOf[Seq[_]].asType) {
            _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[AnyRef].asType)
          }
        }
        .newReturnType(classOf[ShuffleKey].asType)) { implicit mb =>
        val thisVar :: elementsVar :: _ = mb.argVars

        elementsVar.push().invokeI("size", Type.INT_TYPE).unlessEq(ldc(keyElementTypes.size)) {
          `throw`(pushNewIAE(s"The number of key elements should be ${keyElementTypes.size}."))
        }

        keyElementTypes.zipWithIndex.foreach {
          case (t, i) =>
            applySeq(elementsVar.push(), ldc(i)).isInstanceOf(t).unlessTrue {
              val className = t.getClassName
              val idx = className.lastIndexOf('.')
              val simpleName = if (idx >= 0) {
                className.substring(idx + 1)
              } else {
                className
              }
              `throw`(pushNewIAE(s"The key elements(${i}) should be ${simpleName}."))
            }
        }

        val shuffleKey = pushNew(classOf[ShuffleKey].asType)
        shuffleKey.dup().invokeInit(
          pushObject(WritableSerDe)
            .invokeV("serialize", classOf[Array[Byte]].asType, elementsVar.push()))
        `return`(shuffleKey)
      }
  }

  private def pushNewIAE(message: String)(implicit mb: MethodBuilder): Stack = {
    val ex = pushNew(classOf[IllegalArgumentException].asType)
    ex.dup().invokeInit(ldc(message))
    ex
  }
}

object MapGroupViewClassBuilder {

  private[this] val curIds: mutable.Map[CompilerContext, AtomicLong] =
    mutable.WeakHashMap.empty

  def nextId(implicit context: CompilerContext): Long =
    curIds.getOrElseUpdate(context, new AtomicLong(0L)).getAndIncrement()

  private[this] val cache: mutable.Map[CompilerContext, mutable.Map[Seq[Type], Type]] = // scalastyle:ignore
    mutable.WeakHashMap.empty

  def getOrCompile(
    keyElementTypes: Seq[Type])(
      implicit context: CompilerContext): Type = {
    cache.getOrElseUpdate(context, mutable.Map.empty)
      .getOrElseUpdate(
        keyElementTypes,
        context.addClass(new MapGroupViewClassBuilder(keyElementTypes)))
  }
}
