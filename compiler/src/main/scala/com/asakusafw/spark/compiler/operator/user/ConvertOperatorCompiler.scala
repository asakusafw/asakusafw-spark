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
package user

import scala.reflect.ClassTag

import org.objectweb.asm.Type
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.lang.compiler.model.graph.{ OperatorInput, UserOperator }
import com.asakusafw.runtime.core.GroupView
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.spark.compiler.spi.{ OperatorCompiler, OperatorType }
import com.asakusafw.spark.runtime.fragment.user.ConvertOperatorFragment
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._
import com.asakusafw.vocabulary.operator.Convert

class ConvertOperatorCompiler extends UserOperatorCompiler {

  override def support(
    operator: UserOperator)(
      implicit context: OperatorCompiler.Context): Boolean = {
    operator.annotationDesc.resolveClass == classOf[Convert]
  }

  override def operatorType: OperatorType = OperatorType.ExtractType

  override def compile(
    operator: UserOperator)(
      implicit context: OperatorCompiler.Context): Type = {

    assert(support(operator),
      s"The operator type is not supported: ${operator.annotationDesc.resolveClass.getSimpleName}"
        + s" [${operator}]")
    assert(operator.inputs.size >= 1,
      "The size of inputs should be greater than or equals to 1: " +
        s"${operator.inputs.size} [${operator}]")
    assert(operator.outputs.size == 2,
      s"The size of outputs should be 2: ${operator.outputs.size} [${operator}]")

    assert(
      operator.methodDesc.parameterClasses
        .zip(operator.inputs.take(1).map(_.dataModelClass)
          ++: operator.inputs.drop(1).collect {
            case input: OperatorInput if input.getInputUnit == OperatorInput.InputUnit.WHOLE =>
              classOf[GroupView[_]]
          }
          ++: operator.arguments.map(_.resolveClass))
        .forall {
          case (method, model) => method.isAssignableFrom(model)
        },
      s"The operator method parameter types are not compatible: (${
        operator.methodDesc.parameterClasses.map(_.getName).mkString("(", ",", ")")
      }, ${
        (operator.inputs.take(1).map(_.dataModelClass)
          ++: operator.inputs.drop(1).collect {
            case input: OperatorInput if input.getInputUnit == OperatorInput.InputUnit.WHOLE =>
              classOf[GroupView[_]]
          }
          ++: operator.arguments.map(_.resolveClass)).map(_.getName).mkString("(", ",", ")")
      }) [${operator}]")

    val builder = new ConvertOperatorFragmentClassBuilder(operator)

    context.addClass(builder)
  }
}

private class ConvertOperatorFragmentClassBuilder(
  operator: UserOperator)(
    implicit context: OperatorCompiler.Context)
  extends UserOperatorFragmentClassBuilder(
    operator.inputs(Convert.ID_INPUT).dataModelType,
    operator.implementationClass.asType,
    operator.inputs,
    operator.outputs)(
    Option(
      new ClassSignatureBuilder()
        .newSuperclass {
          _.newClassType(classOf[ConvertOperatorFragment[_, _]].asType) {
            _.newTypeArgument(
              SignatureVisitor.INSTANCEOF,
              operator.inputs(Convert.ID_INPUT).dataModelType)
              .newTypeArgument(
                SignatureVisitor.INSTANCEOF,
                operator.outputs(Convert.ID_OUTPUT_CONVERTED).dataModelType)
          }
        }),
    classOf[ConvertOperatorFragment[_, _]].asType) {

  override def defCtor()(implicit mb: MethodBuilder): Unit = {
    val thisVar :: _ :: fragmentVars = mb.argVars

    thisVar.push().invokeInit(
      superType,
      fragmentVars(Convert.ID_OUTPUT_ORIGINAL).push(),
      fragmentVars(Convert.ID_OUTPUT_CONVERTED).push())
  }

  override def defMethods(methodDef: MethodDef): Unit = {
    super.defMethods(methodDef)

    methodDef.newMethod(
      "convert",
      classOf[DataModel[_]].asType,
      Seq(classOf[DataModel[_]].asType),
      new MethodSignatureBuilder()
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
        val thisVar :: inputVar :: _ = mb.argVars
        `return`(
          thisVar.push().invokeV(
            "convert",
            operator.outputs(Convert.ID_OUTPUT_CONVERTED).dataModelType,
            inputVar.push().cast(dataModelType)))
      }

    methodDef.newMethod(
      "convert",
      operator.outputs(Convert.ID_OUTPUT_CONVERTED).dataModelType,
      Seq(dataModelType)) { implicit mb =>
        val thisVar :: inputVar :: _ = mb.argVars
        `return`(
          getOperatorField()
            .invokeV(
              operator.methodDesc.getName,
              operator.outputs(Convert.ID_OUTPUT_CONVERTED).dataModelType,
              (inputVar.push()
                +: operator.inputs.drop(1).collect {
                  case input: OperatorInput if input.getInputUnit == OperatorInput.InputUnit.WHOLE => // scalastyle:ignore
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
                }: _*))
      }
  }
}
