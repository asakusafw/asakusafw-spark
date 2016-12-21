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

import com.asakusafw.bridge.api.Report
import com.asakusafw.lang.compiler.model.graph.UserOperator
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.spark.compiler.spi.{ OperatorCompiler, OperatorType }
import com.asakusafw.spark.runtime.fragment.user.LoggingOperatorFragment
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._
import com.asakusafw.vocabulary.operator.Logging

class LoggingOperatorCompiler extends UserOperatorCompiler {

  override def support(
    operator: UserOperator)(
      implicit context: OperatorCompiler.Context): Boolean = {
    operator.annotationDesc.resolveClass == classOf[Logging]
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
    assert(operator.outputs.size == 1,
      s"The size of outputs should be 1: ${operator.outputs.size} [${operator}]")

    assert(
      operator.methodDesc.parameterClasses
        .zip(operator.inputs.map(_.dataModelClass)
          ++: operator.arguments.map(_.resolveClass))
        .forall {
          case (method, model) => method.isAssignableFrom(model)
        },
      s"The operator method parameter types are not compatible: (${
        operator.methodDesc.parameterClasses.map(_.getName).mkString("(", ",", ")")
      }, ${
        (operator.inputs.map(_.dataModelClass)
          ++: operator.arguments.map(_.resolveClass)).map(_.getName).mkString("(", ",", ")")
      }) [${operator}]")

    val builder = new LoggingOperatorFragmentClassBuilder(operator)

    context.addClass(builder)
  }
}

private class LoggingOperatorFragmentClassBuilder(
  operator: UserOperator)(
    implicit context: OperatorCompiler.Context)
  extends UserOperatorFragmentClassBuilder(
    operator.inputs(Logging.ID_INPUT).dataModelType,
    operator.implementationClass.asType,
    operator.inputs,
    operator.outputs)(
    Option(
      new ClassSignatureBuilder()
        .newSuperclass {
          _.newClassType(classOf[LoggingOperatorFragment[_]].asType) {
            _.newTypeArgument(
              SignatureVisitor.INSTANCEOF,
              operator.inputs(Logging.ID_INPUT).dataModelType)
          }
        }),
    classOf[LoggingOperatorFragment[_]].asType) {

  val level = Option(operator.annotationDesc.elements("value"))
    .map(_.value.asInstanceOf[Logging.Level]).getOrElse(Logging.Level.getDefault)

  override def defCtor()(implicit mb: MethodBuilder): Unit = {
    val thisVar :: _ :: fragmentVars = mb.argVars

    thisVar.push().invokeInit(
      superType,
      fragmentVars(Logging.ID_OUTPUT).push())
  }

  override def defMethods(methodDef: MethodDef): Unit = {
    super.defMethods(methodDef)

    methodDef.newMethod(
      "logging",
      Seq(classOf[DataModel[_]].asType),
      new MethodSignatureBuilder()
        .newParameterType {
          _.newClassType(classOf[DataModel[_]].asType) {
            _.newTypeArgument()
          }
        }
        .newVoidReturnType()) { implicit mb =>
        val thisVar :: inputVar :: _ = mb.argVars
        thisVar.push().invokeV(
          "logging",
          inputVar.push().cast(dataModelType))
        `return`()
      }

    methodDef.newMethod(
      "logging",
      Seq(dataModelType)) { implicit mb =>
        val thisVar :: inputVar :: _ = mb.argVars
        invokeStatic(
          classOf[Report].asType,
          level match {
            case Logging.Level.ERROR => "error"
            case Logging.Level.WARN => "warn"
            case _ => "info"
          },
          getOperatorField()
            .invokeV(
              operator.methodDesc.getName,
              classOf[String].asType,
              inputVar.push().asType(operator.methodDesc.asType.getArgumentTypes()(0))
                +: operator.arguments.map { argument =>
                  Option(argument.value).map { value =>
                    ldc(value)(ClassTag(argument.resolveClass), implicitly)
                  }.getOrElse {
                    pushNull(argument.resolveClass.asType)
                  }
                }: _*))
        `return`()
      }
  }
}
