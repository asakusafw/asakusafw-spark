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
package serializer

import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable

import org.apache.hadoop.io.Writable
import org.objectweb.asm.Type
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.spark.compiler.serializer.WritableSerializerClassBuilder._
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.spark.runtime.serializer.WritableSerializer
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._

class WritableSerializerClassBuilder(
  writableType: Type)(
    implicit context: CompilerContext)
  extends ClassBuilder(
    Type.getType(
      s"L${GeneratedClassPackageInternalName}/${context.flowId}/serializer/WritableSerializer$$${nextId};"), // scalastyle:ignore
    new ClassSignatureBuilder()
      .newSuperclass {
        _.newClassType(classOf[WritableSerializer[_]].asType) {
          _.newTypeArgument(SignatureVisitor.INSTANCEOF, writableType)
        }
      },
    classOf[WritableSerializer[_]].asType) {

  override def defMethods(methodDef: MethodDef): Unit = {
    super.defMethods(methodDef)

    methodDef.newMethod("newInstance", writableType, Seq.empty) { implicit mb =>
      `return`(pushNew0(writableType))
    }

    methodDef.newMethod("newInstance", classOf[Writable].asType, Seq.empty) { implicit mb =>
      val thisVar :: _ = mb.argVars
      `return`(thisVar.push().invokeV("newInstance", writableType))
    }
  }
}

object WritableSerializerClassBuilder {

  private[this] val curId: AtomicLong = new AtomicLong(0L)

  def nextId: Long = curId.getAndIncrement

  private[this] val cache: mutable.Map[CompilerContext, mutable.Map[(String, Type), Type]] =
    mutable.WeakHashMap.empty

  def getOrCompile(
    writableType: Type)(
      implicit context: CompilerContext): Type = {
    cache.getOrElseUpdate(context, mutable.Map.empty).getOrElseUpdate(
      (context.flowId, writableType), {
        context.addClass(new WritableSerializerClassBuilder(writableType))
      })
  }
}
