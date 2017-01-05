/*
 * Copyright 2011-2017 Asakusa Framework Team.
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

import scala.language.existentials
import scala.reflect.ClassTag

import org.objectweb.asm.Type
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.lang.compiler.model.graph.{ OperatorInput, UserOperator }
import com.asakusafw.runtime.core.GroupView
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.spark.compiler.spi.{ OperatorCompiler, OperatorType }
import com.asakusafw.spark.runtime.fragment.user.BranchOperatorFragment
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._
import com.asakusafw.spark.tools.asm4s._
import com.asakusafw.vocabulary.operator.Branch

class BranchOperatorCompiler extends UserOperatorCompiler {

  override def support(
    operator: UserOperator)(
      implicit context: OperatorCompiler.Context): Boolean = {
    operator.annotationDesc.resolveClass == classOf[Branch]
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
    assert(operator.outputs.size > 0,
      s"The size of outputs should be greater than 0: ${operator.outputs.size} [${operator}]")

    assert(
      operator.outputs.forall { output =>
        output.dataModelType == operator.inputs(Branch.ID_INPUT).dataModelType
      },
      "All of output types should be the same: "
        + s"${operator.outputs.map(_.dataModelType).mkString("(", ",", ")")} [${operator}]")

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

    val builder = new BranchOperatorFragmentClassBuilder(operator)

    context.addClass(builder)
  }
}

private class BranchOperatorFragmentClassBuilder(
  operator: UserOperator)(
    implicit context: OperatorCompiler.Context)
  extends UserOperatorFragmentClassBuilder(
    operator.inputs(Branch.ID_INPUT).dataModelType,
    operator.implementationClass.asType,
    operator.inputs,
    operator.outputs)(
    Option(
      new ClassSignatureBuilder()
        .newSuperclass {
          _.newClassType(classOf[BranchOperatorFragment[_, _]].asType) {
            _.newTypeArgument(
              SignatureVisitor.INSTANCEOF,
              operator.inputs(Branch.ID_INPUT).dataModelType)
              .newTypeArgument(
                SignatureVisitor.INSTANCEOF,
                operator.methodDesc.asType.getReturnType)
          }
        }),
    classOf[BranchOperatorFragment[_, _]].asType) {

  override def defCtor()(implicit mb: MethodBuilder): Unit = {
    val thisVar :: _ :: fragmentVars = mb.argVars

    val branchOutputMap = operator.branchOutputMap

    thisVar.push().invokeInit(
      superType,
      buildMap { builder =>
        operatorOutputs.zip(fragmentVars).foreach {
          case (output, fragmentVar) =>
            val enum = branchOutputMap(output)
            builder += (
              getStatic(
                operator.methodDesc.asType.getReturnType,
                enum.name,
                operator.methodDesc.asType.getReturnType),
                fragmentVar.push())
        }
      })
  }

  override def defMethods(methodDef: MethodDef): Unit = {
    super.defMethods(methodDef)

    methodDef.newMethod(
      "branch",
      classOf[Enum[_]].asType,
      Seq(classOf[DataModel[_]].asType),
      new MethodSignatureBuilder()
        .newParameterType {
          _.newClassType(classOf[DataModel[_]].asType) {
            _.newTypeArgument()
          }
        }
        .newReturnType {
          _.newClassType(classOf[Enum[_]].asType) {
            _.newTypeArgument()
          }
        }) { implicit mb =>
        val thisVar :: dmVar :: _ = mb.argVars
        `return`(
          thisVar.push().invokeV(
            "branch",
            operator.methodDesc.asType.getReturnType,
            dmVar.push().cast(dataModelType)))
      }

    methodDef.newMethod(
      "branch",
      operator.methodDesc.asType.getReturnType,
      Seq(dataModelType)) { implicit mb =>
        val thisVar :: dmVar :: _ = mb.argVars

        val branch = getOperatorField()
          .invokeV(
            operator.methodDesc.name,
            operator.methodDesc.asType.getReturnType,
            (dmVar.push()
              +: operator.inputs.drop(1).collect {
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
        branch.dup().unlessNotNull {
          `throw`(pushNew0(classOf[NullPointerException].asType))
        }

        `return`(branch)
      }
  }
}
