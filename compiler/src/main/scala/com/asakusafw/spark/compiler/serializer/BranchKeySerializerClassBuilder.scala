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
package serializer

import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable

import org.apache.hadoop.io.Writable
import com.esotericsoftware.kryo._
import com.esotericsoftware.kryo.io._
import org.objectweb.asm.Type
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.runtime.model.DataModel
import com.asakusafw.spark.runtime.rdd.BranchKey
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._

class BranchKeySerializerClassBuilder(
  branchKeysType: Type)(
    implicit context: CompilerContext)
  extends ClassBuilder(
    Type.getType(
      s"L${GeneratedClassPackageInternalName}/${context.flowId}/serializer/BranchKeySerializer;"),
    new ClassSignatureBuilder()
      .newSuperclass {
        _.newClassType(classOf[Serializer[_]].asType) {
          _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[BranchKey].asType)
        }
      },
    classOf[Serializer[_]].asType) {

  override def defMethods(methodDef: MethodDef): Unit = {
    super.defMethods(methodDef)

    methodDef.newMethod(
      "write",
      Seq(classOf[Kryo].asType, classOf[Output].asType, classOf[AnyRef].asType)) { implicit mb =>
        val thisVar :: kryoVar :: outputVar :: objVar :: _ = mb.argVars

        thisVar.push()
          .invokeV(
            "write",
            kryoVar.push(),
            outputVar.push(),
            objVar.push().cast(classOf[BranchKey].asType))
        `return`()
      }

    methodDef.newMethod(
      "write",
      Seq(classOf[Kryo].asType, classOf[Output].asType, classOf[BranchKey].asType)) { implicit mb =>
        val thisVar :: kryoVar :: outputVar :: branchKeyVar :: _ = mb.argVars

        outputVar.push().invokeV("writeInt", Type.INT_TYPE,
          branchKeyVar.push().invokeV("id", Type.INT_TYPE), ldc(true))
          .pop()
        `return`()
      }

    methodDef.newMethod(
      "read",
      classOf[AnyRef].asType,
      Seq(classOf[Kryo].asType, classOf[Input].asType, classOf[Class[_]].asType)) { implicit mb =>
        val thisVar :: kryoVar :: inputVar :: classVar :: _ = mb.argVars

        `return`(
          thisVar.push()
            .invokeV(
              "read",
              classOf[BranchKey].asType,
              kryoVar.push(),
              inputVar.push(),
              classVar.push()))
      }

    methodDef.newMethod(
      "read",
      classOf[BranchKey].asType,
      Seq(classOf[Kryo].asType, classOf[Input].asType, classOf[Class[_]].asType)) { implicit mb =>
        val thisVar :: kryoVar :: inputVar :: classVar :: _ = mb.argVars

        `return`(
          invokeStatic(branchKeysType, "valueOf", classOf[BranchKey].asType,
            inputVar.push().invokeV("readInt", Type.INT_TYPE, ldc(true))))
      }
  }
}
