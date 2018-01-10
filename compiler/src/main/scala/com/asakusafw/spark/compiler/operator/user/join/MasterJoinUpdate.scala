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
package operator.user.join

import scala.reflect.ClassTag

import org.objectweb.asm.Type

import com.asakusafw.lang.compiler.model.graph.{ OperatorInput, UserOperator }
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.spark.compiler.spi.OperatorCompiler
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._
import com.asakusafw.vocabulary.operator.{ MasterJoinUpdate => MasterJoinUpdateOp }

trait MasterJoinUpdate extends JoinOperatorFragmentClassBuilder {

  implicit def context: OperatorCompiler.Context

  override def defMethods(methodDef: MethodDef): Unit = {
    super.defMethods(methodDef)

    methodDef.newMethod(
      "update",
      Seq(classOf[DataModel[_]].asType, classOf[DataModel[_]].asType),
      new MethodSignatureBuilder()
        .newParameterType {
          _.newClassType(classOf[DataModel[_]].asType) {
            _.newTypeArgument()
          }
        }
        .newParameterType {
          _.newClassType(classOf[DataModel[_]].asType) {
            _.newTypeArgument()
          }
        }
        .newVoidReturnType()) { implicit mb =>
        val thisVar :: masterVar :: txVar :: _ = mb.argVars
        thisVar.push().invokeV(
          "update",
          masterVar.push().cast(masterType),
          txVar.push().cast(txType))
        `return`()
      }

    methodDef.newMethod(
      "update",
      Seq(masterType, txType)) { implicit mb =>
        val thisVar :: masterVar :: txVar :: _ = mb.argVars
        getOperatorField()
          .invokeV(
            operator.methodDesc.getName,
            (masterVar.push()
              +: txVar.push()
              +: operator.inputs.drop(2).collect {
                case input: OperatorInput if input.getInputUnit == OperatorInput.InputUnit.WHOLE =>
                  getViewField(input)
              }
              ++: operator.arguments.map { argument =>
                Option(argument.value).map { value =>
                  ldc(value)(ClassTag(argument.resolveClass), implicitly)
                }.getOrElse {
                  pushNull(argument.resolveClass.asType)
                }
              }).zip(operator.methodDesc.asType.getArgumentTypes()).map {
                case (s, t) => s.asType(t)
              }: _*)
        `return`()
      }
  }
}
