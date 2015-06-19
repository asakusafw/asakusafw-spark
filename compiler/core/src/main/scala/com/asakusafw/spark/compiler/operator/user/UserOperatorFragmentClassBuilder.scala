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
package operator
package user

import scala.concurrent.Future

import org.apache.spark.broadcast.Broadcast
import org.objectweb.asm.Type
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.lang.compiler.model.graph.OperatorOutput
import com.asakusafw.spark.runtime.driver.BroadcastId
import com.asakusafw.spark.runtime.fragment.Fragment
import com.asakusafw.spark.tools.asm._

abstract class UserOperatorFragmentClassBuilder(
  flowId: String,
  dataModelType: Type,
  val operatorType: Type,
  val operatorOutputs: Seq[OperatorOutput])
    extends FragmentClassBuilder(flowId, dataModelType)
    with OperatorField
    with OutputFragments {

  override def defConstructors(ctorDef: ConstructorDef): Unit = {
    ctorDef.newInit(
      classOf[Map[BroadcastId, Broadcast[_]]].asType +: (0 until operatorOutputs.size).map(_ => classOf[Fragment[_]].asType),
      ((new MethodSignatureBuilder()
        .newParameterType {
          _.newClassType(classOf[Map[_, _]].asType) {
            _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[BroadcastId].asType)
              .newTypeArgument(SignatureVisitor.INSTANCEOF) {
                _.newClassType(classOf[Broadcast[_]].asType) {
                  _.newTypeArgument()
                }
              }
          }
        } /: operatorOutputs) {
          case (builder, output) =>
            builder.newParameterType {
              _.newClassType(classOf[Fragment[_]].asType) {
                _.newTypeArgument(SignatureVisitor.INSTANCEOF, output.getDataType.asType)
              }
            }
        })
        .newVoidReturnType()
        .build()) { mb =>
        import mb._
        val broadcastsVar = `var`(classOf[Map[BroadcastId, Broadcast[_]]].asType, thisVar.nextLocal)

        thisVar.push().invokeInit(superType)
        initReset(mb)
        initOutputFields(mb, broadcastsVar.nextLocal)
        initOperatorField(mb)
        initFields(mb)
      }
  }

  def initFields(mb: MethodBuilder): Unit = {}
}
