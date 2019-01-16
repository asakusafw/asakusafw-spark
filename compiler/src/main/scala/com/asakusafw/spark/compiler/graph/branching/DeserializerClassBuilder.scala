/*
 * Copyright 2011-2019 Asakusa Framework Team.
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
package graph.branching

import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable
import scala.runtime.AbstractFunction1

import org.apache.hadoop.io.Writable
import org.objectweb.asm.{ Opcodes, Type }
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.spark.compiler.graph.branching.DeserializerClassBuilder._
import com.asakusafw.spark.runtime.io.WritableSerDe
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._
import com.asakusafw.spark.tools.asm4s._

class DeserializerClassBuilder(
  dataModelType: Type,
  forBroadcast: Boolean)(
    implicit val context: CompilerContext)
  extends ClassBuilder(
    Type.getType(
      s"L${GeneratedClassPackageInternalName}/${context.flowId}/graph/Deserializer$$${nextId};"), // scalastyle:ignore
    new ClassSignatureBuilder()
      .newSuperclass {
        _.newClassType(classOf[AbstractFunction1[_, _]].asType) {
          _
            .newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[Array[Byte]].asType)
            .newTypeArgument(SignatureVisitor.INSTANCEOF, dataModelType)
        }
      }
      .newInterface {
        _.newClassType(classOf[Function1[_, _]].asType) {
          _
            .newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[Array[Byte]].asType)
            .newTypeArgument(SignatureVisitor.INSTANCEOF, dataModelType)
        }
      },
    classOf[AbstractFunction1[_, _]].asType,
    classOf[Function1[_, _]].asType) {

  override def defFields(fieldDef: FieldDef): Unit = {
    super.defFields(fieldDef)

    if (!forBroadcast) {
      fieldDef.newField(Opcodes.ACC_PRIVATE, "value", dataModelType)
    }
  }

  override def defConstructors(ctorDef: ConstructorDef): Unit = {
    ctorDef.newInit(Seq.empty) { implicit mb =>
      val thisVar :: _ = mb.argVars
      thisVar.push().invokeInit(superType)

      if (!forBroadcast) {
        thisVar.push().putField("value", pushNew0(dataModelType))
      }
    }
  }

  override def defMethods(methodDef: MethodDef): Unit = {
    super.defMethods(methodDef)

    methodDef.newMethod(
      "apply",
      classOf[AnyRef].asType,
      Seq(classOf[AnyRef].asType)) { implicit mb =>
        val thisVar :: bytesVar :: _ = mb.argVars

        `return`(
          thisVar.push()
            .invokeV("apply", dataModelType, bytesVar.push().cast(classOf[Array[Byte]].asType)))
      }

    methodDef.newMethod(
      "apply",
      dataModelType,
      Seq(classOf[Array[Byte]].asType)) { implicit mb =>
        val thisVar :: bytesVar :: _ = mb.argVars

        val valueVar = (if (forBroadcast) {
          pushNew0(dataModelType)
        } else {
          thisVar.push().getField("value", dataModelType)
        }).store()
        pushObject(WritableSerDe)
          .invokeV(
            "deserialize",
            bytesVar.push(),
            valueVar.push().asType(classOf[Writable].asType))
        `return`(valueVar.push())
      }
  }
}

object DeserializerClassBuilder {

  private[this] val curIds: mutable.Map[CompilerContext, AtomicLong] =
    mutable.WeakHashMap.empty

  def nextId(implicit context: CompilerContext): Long =
    curIds.getOrElseUpdate(context, new AtomicLong(0)).getAndIncrement()

  private[this] val cache: mutable.Map[CompilerContext, mutable.Map[(Type, Boolean), Type]] =
    mutable.WeakHashMap.empty

  def getOrCompile(
    dataModelType: Type, forBroadcast: Boolean)(
      implicit context: CompilerContext): Type = {
    cache.getOrElseUpdate(context, mutable.Map.empty)
      .getOrElseUpdate(
        (dataModelType, forBroadcast),
        context.addClass(new DeserializerClassBuilder(dataModelType, forBroadcast)))
  }
}
