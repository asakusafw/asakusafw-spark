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
package operator
package core

import org.apache.spark.broadcast.{ Broadcast => Broadcasted }
import org.objectweb.asm.Type
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.runtime.model.DataModel
import com.asakusafw.spark.compiler.spi.OperatorCompiler
import com.asakusafw.spark.runtime.fragment.Fragment
import com.asakusafw.spark.runtime.graph.BroadcastId
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._

abstract class CoreOperatorFragmentClassBuilder(
  dataModelType: Type,
  val childDataModelType: Type)(
    signature: Option[ClassSignatureBuilder],
    superType: Type)(
      implicit context: OperatorCompiler.Context)
  extends FragmentClassBuilder(dataModelType)(signature, superType) {

  override final def defConstructors(ctorDef: ConstructorDef): Unit = {
    ctorDef.newInit(
      Seq(classOf[Map[BroadcastId, Broadcasted[_]]].asType, classOf[Fragment[_]].asType),
      new MethodSignatureBuilder()
        .newParameterType {
          _.newClassType(classOf[Map[_, _]].asType) {
            _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[BroadcastId].asType)
              .newTypeArgument(SignatureVisitor.INSTANCEOF) {
                _.newClassType(classOf[Broadcasted[_]].asType) {
                  _.newTypeArgument()
                }
              }
          }
        }
        .newParameterType {
          _.newClassType(classOf[Fragment[_]].asType) {
            _.newTypeArgument(SignatureVisitor.INSTANCEOF, childDataModelType)
          }
        }
        .newVoidReturnType())(defCtor()(_))
  }

  def defCtor()(implicit mb: MethodBuilder): Unit
}
