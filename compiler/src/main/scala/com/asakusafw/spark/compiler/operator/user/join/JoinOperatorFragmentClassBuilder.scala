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
package operator.user
package join

import java.util.{ List => JList }

import scala.reflect.ClassTag

import org.objectweb.asm.Type
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.lang.compiler.model.graph.{ OperatorInput, OperatorOutput, UserOperator }
import com.asakusafw.runtime.flow.ListBuffer
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.spark.compiler.spi.OperatorCompiler
import com.asakusafw.spark.runtime.operator.DefaultMasterSelection
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._
import com.asakusafw.spark.tools.asm4s._

abstract class JoinOperatorFragmentClassBuilder(
  dataModelType: Type,
  val operator: UserOperator,
  val masterInput: OperatorInput,
  val txInput: OperatorInput)(
    signature: Option[ClassSignatureBuilder],
    superType: Type)(
      implicit context: OperatorCompiler.Context)
  extends UserOperatorFragmentClassBuilder(
    dataModelType,
    operator.implementationClass.asType,
    operator.inputs,
    operator.outputs)(signature, superType) {

  val masterType: Type = masterInput.dataModelType
  val txType: Type = txInput.dataModelType

  override def defMethods(methodDef: MethodDef): Unit = {
    super.defMethods(methodDef)

    methodDef.newMethod(
      "masterSelection",
      classOf[DataModel[_]].asType,
      Seq(classOf[JList[_]].asType, classOf[DataModel[_]].asType),
      new MethodSignatureBuilder()
        .newParameterType {
          _.newClassType(classOf[JList[_]].asType) {
            _.newTypeArgument(SignatureVisitor.INSTANCEOF) {
              _.newClassType(classOf[DataModel[_]].asType) {
                _.newTypeArgument()
              }
            }
          }
        }
        .newParameterType {
          _.newClassType(classOf[DataModel[_]].asType) {
            _.newTypeArgument()
          }
        }
        .newReturnType {
          _.newClassType(classOf[DataModel[_]].asType) {
            _.newTypeArgument()
          }
        }) { implicit mb =>
        val thisVar :: mastersVar :: txVar :: _ = mb.argVars
        `return`(
          thisVar.push().invokeV(
            "masterSelection",
            masterType,
            mastersVar.push(),
            txVar.push().cast(txType)))
      }

    methodDef.newMethod(
      "masterSelection",
      masterType,
      Seq(classOf[JList[_]].asType, txType),
      new MethodSignatureBuilder()
        .newParameterType {
          _.newClassType(classOf[JList[_]].asType) {
            _.newTypeArgument(SignatureVisitor.INSTANCEOF, masterType)
          }
        }
        .newParameterType(txType)
        .newReturnType(masterType)) { implicit mb =>
        val thisVar :: mastersVar :: txVar :: _ = mb.argVars
        `return`(
          operator.selectionMethod match {
            case Some((name, t)) =>
              getOperatorField()
                .invokeV(
                  name,
                  t.getReturnType(),
                  ({ () => mastersVar.push() }
                    +: { () => txVar.push() }
                    +: operator.inputs.drop(2).collect {
                      case input: OperatorInput if input.getInputUnit == OperatorInput.InputUnit.WHOLE => // scalastyle:ignore
                        () => getViewField(input)
                    }
                    ++: operator.arguments.map { argument =>
                      Option(argument.value).map { value =>
                        () => ldc(value)(ClassTag(argument.resolveClass), implicitly)
                      }.getOrElse {
                        () => pushNull(argument.resolveClass.asType)
                      }
                    }).zip(t.getArgumentTypes()).map {
                      case (s, t) => s().asType(t)
                    }: _*)
                .cast(masterType)
            case None =>
              pushObject(DefaultMasterSelection)
                .invokeV(
                  "select",
                  classOf[AnyRef].asType,
                  mastersVar.push(),
                  txVar.push().asType(classOf[AnyRef].asType))
                .cast(masterType)
          })
      }
  }
}
