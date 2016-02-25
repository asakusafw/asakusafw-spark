/*
 * Copyright 2011-2016 Asakusa Framework Team.
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

import scala.reflect.ClassTag

import org.objectweb.asm.Type
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.lang.compiler.model.graph.UserOperator
import com.asakusafw.runtime.core.Result
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.spark.compiler.spi.{ OperatorCompiler, OperatorType }
import com.asakusafw.spark.runtime.fragment.user.ExtractOperatorFragment
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._
import com.asakusafw.spark.tools.asm4s._
import com.asakusafw.vocabulary.operator.Extract

class ExtractOperatorCompiler extends UserOperatorCompiler {

  override def support(
    operator: UserOperator)(
      implicit context: OperatorCompiler.Context): Boolean = {
    operator.annotationDesc.resolveClass == classOf[Extract]
  }

  override def operatorType: OperatorType = OperatorType.ExtractType

  override def compile(
    operator: UserOperator)(
      implicit context: OperatorCompiler.Context): Type = {

    assert(support(operator),
      s"The operator type is not supported: ${operator.annotationDesc.resolveClass.getSimpleName}"
        + s" [${operator}]")
    assert(operator.inputs.size == 1, // FIXME to take multiple inputs for side data?
      s"The size of inputs should be 1: ${operator.inputs.size} [${operator}]")
    assert(operator.outputs.size > 0,
      s"The size of outputs should be greater than 0: ${operator.outputs.size} [${operator}]")

    assert(
      operator.methodDesc.parameterClasses
        .zip(operator.inputs.map(_.dataModelClass)
          ++: operator.outputs.map(_ => classOf[Result[_]])
          ++: operator.arguments.map(_.resolveClass))
        .forall {
          case (method, model) => method.isAssignableFrom(model)
        },
      s"The operator method parameter types are not compatible: (${
        operator.methodDesc.parameterClasses.map(_.getName).mkString("(", ",", ")")
      }, ${
        (operator.inputs.map(_.dataModelClass)
          ++: operator.outputs.map(_ => classOf[Result[_]])
          ++: operator.arguments.map(_.resolveClass)).map(_.getName).mkString("(", ",", ")")
      }) [${operator}]")

    val builder = new ExtractOperatorFragmentClassBuilder(operator)

    context.addClass(builder)
  }
}

private class ExtractOperatorFragmentClassBuilder(
  operator: UserOperator)(
    implicit context: OperatorCompiler.Context)
  extends UserOperatorFragmentClassBuilder(
    operator.inputs(Extract.ID_INPUT).dataModelType,
    operator.implementationClass.asType,
    operator.outputs)(
    Option(
      new ClassSignatureBuilder()
        .newSuperclass {
          _.newClassType(classOf[ExtractOperatorFragment[_]].asType) {
            _.newTypeArgument(
              SignatureVisitor.INSTANCEOF,
              operator.inputs(Extract.ID_INPUT).dataModelType)
          }
        }),
    classOf[ExtractOperatorFragment[_]].asType) {

  override def defCtor()(implicit mb: MethodBuilder): Unit = {
    val thisVar :: _ :: fragmentVars = mb.argVars

    thisVar.push().invokeInit(
      superType,
      buildIndexedSeq { builder =>
        fragmentVars.foreach {
          builder += _.push()
        }
      })
  }

  override def defMethods(methodDef: MethodDef): Unit = {
    super.defMethods(methodDef)

    methodDef.newMethod(
      "extract",
      Seq(classOf[DataModel[_]].asType, classOf[IndexedSeq[Result[_]]].asType),
      new MethodSignatureBuilder()
        .newParameterType {
          _.newClassType(classOf[DataModel[_]].asType) {
            _.newTypeArgument()
          }
        }
        .newParameterType {
          _.newClassType(classOf[IndexedSeq[Result[_]]].asType) {
            _.newTypeArgument(SignatureVisitor.INSTANCEOF) {
              _.newClassType(classOf[Result[_]].asType) {
                _.newTypeArgument()
              }
            }
          }
        }
        .newVoidReturnType()) { implicit mb =>
        val thisVar :: inputVar :: outputsVar :: _ = mb.argVars
        thisVar.push().invokeV(
          "extract",
          inputVar.push().cast(dataModelType),
          outputsVar.push())
        `return`()
      }

    methodDef.newMethod(
      "extract",
      Seq(dataModelType, classOf[IndexedSeq[Result[_]]].asType),
      new MethodSignatureBuilder()
        .newParameterType(dataModelType)
        .newParameterType {
          _.newClassType(classOf[IndexedSeq[Result[_]]].asType) {
            _.newTypeArgument(SignatureVisitor.INSTANCEOF) {
              _.newClassType(classOf[Result[_]].asType) {
                _.newTypeArgument()
              }
            }
          }
        }
        .newVoidReturnType()) { implicit mb =>
        val thisVar :: inputVar :: outputsVar :: _ = mb.argVars

        getOperatorField()
          .invokeV(
            operator.methodDesc.getName,
            inputVar.push().asType(operator.methodDesc.asType.getArgumentTypes()(0))
              +: (0 until operator.outputs.size).map { i =>
                applySeq(outputsVar.push(), ldc(i)).asType(classOf[Result[_]].asType)
              }
              ++: operator.arguments.map { argument =>
                Option(argument.value).map { value =>
                  ldc(value)(ClassTag(argument.resolveClass), implicitly)
                }.getOrElse {
                  pushNull(argument.resolveClass.asType)
                }
              }: _*)

        `return`()
      }
  }
}
