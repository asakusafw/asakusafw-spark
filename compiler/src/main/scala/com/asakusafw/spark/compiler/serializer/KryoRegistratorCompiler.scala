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

import com.esotericsoftware.kryo.{ Kryo, Registration, Serializer }

import org.objectweb.asm.Type

import com.asakusafw.spark.runtime.driver.BroadcastId
import com.asakusafw.spark.runtime.rdd.BranchKey
import com.asakusafw.spark.runtime.serializer.KryoRegistrator
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._

object KryoRegistratorCompiler {

  def compile(
    writables: Set[Type],
    branchKeySerializer: Type,
    broadcastIdSerializer: Type)(
      implicit context: CompilerContext): Type = {
    val serializers = writables.map { writable =>
      writable -> WritableSerializerClassBuilder.getOrCompile(writable)
    }

    val builder =
      new KryoRegistratorClassBuilder(
        serializers,
        branchKeySerializer,
        broadcastIdSerializer)

    context.addClass(builder)
  }
}

private class KryoRegistratorClassBuilder(
  serializers: Set[(Type, Type)],
  branchKeySerializer: Type,
  broadcastIdSerializer: Type)(
    implicit context: CompilerContext)
  extends ClassBuilder(
    Type.getType(
      s"L${GeneratedClassPackageInternalName}/${context.flowId}/serializer/KryoRegistrator;"),
    classOf[KryoRegistrator].asType) {

  override def defMethods(methodDef: MethodDef): Unit = {
    super.defMethods(methodDef)

    methodDef.newMethod("registerClasses", Seq(classOf[Kryo].asType)) { implicit mb =>
      val kryoVar = `var`(classOf[Kryo].asType, thisVar.nextLocal)
      thisVar.push().invokeS(classOf[KryoRegistrator].asType, "registerClasses", kryoVar.push())

      serializers.foreach {
        case (dataModelType, serializerType) =>
          kryoVar.push().invokeV(
            "register",
            classOf[Registration].asType,
            ldc(dataModelType).asType(classOf[Class[_]].asType),
            pushNew0(serializerType).asType(classOf[Serializer[_]].asType)).pop()
      }

      kryoVar.push().invokeV(
        "register",
        classOf[Registration].asType,
        ldc(classOf[BranchKey].asType).asType(classOf[Class[_]].asType),
        pushNew0(branchKeySerializer).asType(classOf[Serializer[_]].asType)).pop()

      kryoVar.push().invokeV(
        "register",
        classOf[Registration].asType,
        ldc(classOf[BroadcastId].asType).asType(classOf[Class[_]].asType),
        pushNew0(broadcastIdSerializer).asType(classOf[Serializer[_]].asType)).pop()

      `return`()
    }
  }
}
