/*
 * Copyright 2011-2018 Asakusa Framework Team.
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
package graph

import scala.concurrent.{ ExecutionContext, Future }

import org.objectweb.asm.Opcodes
import org.objectweb.asm.signature.SignatureVisitor

import org.apache.spark.rdd.RDD

import com.asakusafw.spark.runtime.RoundContext
import com.asakusafw.spark.runtime.graph.{ CacheOnce => CacheOnceTrait }
import com.asakusafw.spark.runtime.rdd.BranchKey
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm4s._
import com.asakusafw.spark.tools.asm4s.MixIn._

trait CacheStrategy extends ClassBuilder with Mixing

trait CacheOnce extends CacheStrategy {

  override val mixins = Seq(
    MixIn(classOf[CacheOnceTrait[_, _]].asType,
      Seq(
        FieldDef(Opcodes.ACC_TRANSIENT,
          "value",
          classOf[AnyRef].asType)),
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
