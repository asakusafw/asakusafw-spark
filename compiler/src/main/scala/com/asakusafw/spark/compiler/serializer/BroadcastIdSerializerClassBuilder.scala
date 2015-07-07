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
import com.esotericsoftware.kryo._
import com.esotericsoftware.kryo.io._
import org.objectweb.asm.Type
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.lang.compiler.api.JobflowProcessor.{ Context => JPContext }
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.spark.runtime.driver.BroadcastId
import com.asakusafw.spark.tools.asm._

class BroadcastIdSerializerClassBuilder(
  flowId: String,
  broadcastIdsType: Type)
  extends ClassBuilder(
    Type.getType(
      s"L${GeneratedClassPackageInternalName}/${flowId}/serializer/BroadcastIdSerializer;"),
    new ClassSignatureBuilder()
      .newSuperclass {
        _.newClassType(classOf[Serializer[_]].asType) {
          _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[BroadcastId].asType)
        }
      }
      .build(),
    classOf[Serializer[_]].asType) {

  override def defMethods(methodDef: MethodDef): Unit = {
    super.defMethods(methodDef)

    methodDef.newMethod(
      "write",
      Seq(classOf[Kryo].asType, classOf[Output].asType, classOf[AnyRef].asType)) { mb =>
        import mb._ // scalastyle:ignore
        val kryoVar = `var`(classOf[Kryo].asType, thisVar.nextLocal)
        val outputVar = `var`(classOf[Output].asType, kryoVar.nextLocal)
        val objVar = `var`(classOf[AnyRef].asType, outputVar.nextLocal)
        thisVar.push()
          .invokeV(
            "write",
            kryoVar.push(),
            outputVar.push(),
            objVar.push().cast(classOf[BroadcastId].asType))
        `return`()
      }

    methodDef.newMethod(
      "write",
      Seq(classOf[Kryo].asType, classOf[Output].asType, classOf[BroadcastId].asType)) { mb =>
        import mb._ // scalastyle:ignore
        val kryoVar = `var`(classOf[Kryo].asType, thisVar.nextLocal)
        val outputVar = `var`(classOf[Output].asType, kryoVar.nextLocal)
        val broadcastIdVar = `var`(classOf[BroadcastId].asType, outputVar.nextLocal)
        outputVar.push().invokeV("writeInt", Type.INT_TYPE,
          broadcastIdVar.push().invokeV("id", Type.INT_TYPE), ldc(true))
          .pop()
        `return`()
      }

    methodDef.newMethod(
      "read",
      classOf[AnyRef].asType,
      Seq(classOf[Kryo].asType, classOf[Input].asType, classOf[Class[_]].asType)) { mb =>
        import mb._ // scalastyle:ignore
        val kryoVar = `var`(classOf[Kryo].asType, thisVar.nextLocal)
        val inputVar = `var`(classOf[Input].asType, kryoVar.nextLocal)
        val classVar = `var`(classOf[Class[_]].asType, inputVar.nextLocal)
        `return`(
          thisVar.push().invokeV(
            "read",
            classOf[BroadcastId].asType,
            kryoVar.push(),
            inputVar.push(),
            classVar.push()))
      }

    methodDef.newMethod(
      "read",
      classOf[BroadcastId].asType,
      Seq(classOf[Kryo].asType, classOf[Input].asType, classOf[Class[_]].asType)) { mb =>
        import mb._ // scalastyle:ignore
        val kryoVar = `var`(classOf[Kryo].asType, thisVar.nextLocal)
        val inputVar = `var`(classOf[Input].asType, kryoVar.nextLocal)
        val classVar = `var`(classOf[Class[_]].asType, inputVar.nextLocal)
        `return`(
          invokeStatic(broadcastIdsType, "valueOf", classOf[BroadcastId].asType,
            inputVar.push().invokeV("readInt", Type.INT_TYPE, ldc(true))))
      }
  }
}