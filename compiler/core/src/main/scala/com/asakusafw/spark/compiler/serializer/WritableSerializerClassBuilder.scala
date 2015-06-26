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
package serializer

import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable

import org.apache.hadoop.io.Writable
import org.objectweb.asm.Type
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.lang.compiler.api.JobflowProcessor.{ Context => JPContext }
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.spark.runtime.serializer.WritableSerializer
import com.asakusafw.spark.tools.asm._

class WritableSerializerClassBuilder(flowId: String, writableType: Type)
    extends ClassBuilder(
      Type.getType(s"L${GeneratedClassPackageInternalName}/${flowId}/serializer/WritableSerializer$$${WritableSerializerClassBuilder.nextId};"),
      new ClassSignatureBuilder()
        .newSuperclass {
          _.newClassType(classOf[WritableSerializer[_]].asType) {
            _.newTypeArgument(SignatureVisitor.INSTANCEOF, writableType)
          }
        }
        .build(),
      classOf[WritableSerializer[_]].asType) {

  override def defMethods(methodDef: MethodDef): Unit = {
    super.defMethods(methodDef)

    methodDef.newMethod("newInstance", writableType, Seq.empty) { mb =>
      import mb._ // scalastyle:ignore
      `return`(pushNew0(writableType))
    }

    methodDef.newMethod("newInstance", classOf[Writable].asType, Seq.empty) { mb =>
      import mb._ // scalastyle:ignore
      `return`(thisVar.push().invokeV("newInstance", writableType))
    }
  }
}

object WritableSerializerClassBuilder {

  private[this] val curId: AtomicLong = new AtomicLong(0L)

  def nextId: Long = curId.getAndIncrement

  private[this] val cache: mutable.Map[JPContext, mutable.Map[(String, Type), Type]] =
    mutable.WeakHashMap.empty

  def getOrCompile(
    flowId: String,
    writableType: Type,
    jpContext: JPContext): Type = {
    cache.getOrElseUpdate(jpContext, mutable.Map.empty).getOrElseUpdate(
      (flowId, writableType), {
        jpContext.addClass(new WritableSerializerClassBuilder(flowId, writableType))
      })
  }
}
