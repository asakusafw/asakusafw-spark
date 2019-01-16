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
package com.asakusafw.spark.extensions.iterativebatch.compiler
package graph

import scala.collection.mutable
import scala.concurrent.{ ExecutionContext, Future }

import org.objectweb.asm.Opcodes
import org.objectweb.asm.signature.SignatureVisitor

import org.apache.spark.rdd.RDD

import com.asakusafw.spark.compiler.graph.CacheStrategy
import com.asakusafw.spark.runtime.RoundContext
import com.asakusafw.spark.runtime.rdd.BranchKey
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._
import com.asakusafw.spark.tools.asm4s._
import com.asakusafw.spark.tools.asm4s.MixIn._

import com.asakusafw.spark.extensions.iterativebatch.runtime.graph.{
  CacheAlways => CacheAlwaysTrait,
  CacheByParameter => CacheByParameterTrait
}

trait CacheAlways extends CacheStrategy {

  override val mixins = Seq(
    MixIn(classOf[CacheAlwaysTrait[_, _]].asType,
      Seq(
        FieldDef(
          Opcodes.ACC_FINAL | Opcodes.ACC_TRANSIENT,
          "values",
          classOf[mutable.Map[_, _]].asType,
          _.newClassType(classOf[mutable.Map[_, _]].asType) {
            _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[AnyRef].asType)
              .newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[AnyRef].asType)
          })),
      Seq(
        MethodDef("getOrCache",
          classOf[AnyRef].asType,
          Seq(
            classOf[AnyRef].asType,
            classOf[Function0[_]].asType),
          new MethodSignatureBuilder()
            .newParameterType(classOf[AnyRef].asType)
            .newParameterType {
              _.newClassType(classOf[Function0[_]].asType) {
                _.newTypeArgument()
              }
            }
            .newReturnType(classOf[AnyRef].asType)))))
}

trait CacheByParameter extends CacheStrategy {

  override val mixins = Seq(
    MixIn(classOf[CacheByParameterTrait[_]].asType,
      Seq(
        FieldDef(
          Opcodes.ACC_FINAL | Opcodes.ACC_TRANSIENT,
          "values",
          classOf[mutable.Map[_, _]].asType,
          _.newClassType(classOf[mutable.Map[_, _]].asType) {
            _.newTypeArgument(SignatureVisitor.INSTANCEOF) {
              _.newClassType(classOf[Seq[_]].asType) {
                _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[String].asType)
              }
            }
              .newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[AnyRef].asType)
          })),
      Seq(
        MethodDef("getOrCache",
          classOf[AnyRef].asType,
          Seq(
            classOf[RoundContext].asType,
            classOf[Function0[_]].asType),
          new MethodSignatureBuilder()
            .newParameterType(classOf[RoundContext].asType)
            .newParameterType {
              _.newClassType(classOf[Function0[_]].asType) {
                _.newTypeArgument()
              }
            }
            .newReturnType(classOf[AnyRef].asType)))))

  def parameters: Set[String]

  override def defFields(fieldDef: FieldDef): Unit = {
    super.defFields(fieldDef)

    fieldDef.newField(
      Opcodes.ACC_PRIVATE | Opcodes.ACC_TRANSIENT,
      "parameters",
      classOf[Set[String]].asType,
      new TypeSignatureBuilder()
        .newClassType(classOf[Set[_]].asType) {
          _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[String].asType)
        })
  }

  override def defMethods(methodDef: MethodDef): Unit = {
    super.defMethods(methodDef)

    methodDef.newMethod(
      "getOrCache",
      classOf[AnyRef].asType,
      Seq(classOf[AnyRef].asType, classOf[Function0[_]].asType)) { implicit mb =>

        val thisVar :: keyVar :: valueVar :: _ = mb.argVars

        `return`(
          thisVar.push().invokeV(
            "getOrCache",
            classOf[AnyRef].asType,
            keyVar.push().cast(classOf[RoundContext].asType),
            valueVar.push()))
      }

    methodDef.newMethod("parameters", classOf[Set[String]].asType, Seq.empty) { implicit mb =>

      val thisVar :: _ = mb.argVars

      thisVar.push().getField("parameters", classOf[Set[String]].asType).unlessNotNull {
        thisVar.push().putField(
          "parameters",
          buildSet { builder =>
            parameters.foreach { parameter =>
              builder += ldc(parameter)
            }
          })
      }
      `return`(thisVar.push().getField("parameters", classOf[Set[String]].asType))
    }
  }
}
