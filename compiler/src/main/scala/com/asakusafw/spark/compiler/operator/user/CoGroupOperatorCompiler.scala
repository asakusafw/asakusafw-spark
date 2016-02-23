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

import java.util.{ List => JList }

import scala.collection.JavaConversions._
import scala.reflect.ClassTag

import org.objectweb.asm.Type
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.lang.compiler.model.graph.UserOperator
import com.asakusafw.runtime.core.Result
import com.asakusafw.runtime.flow.{ ArrayListBuffer, FileMapListBuffer, ListBuffer }
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.spark.compiler.spi.{ OperatorCompiler, OperatorType }
import com.asakusafw.spark.runtime.fragment.user.CoGroupOperatorFragment
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._
import com.asakusafw.spark.tools.asm4s._
import com.asakusafw.vocabulary.flow.processor.InputBuffer
import com.asakusafw.vocabulary.operator.{ CoGroup, GroupSort }

class CoGroupOperatorCompiler extends UserOperatorCompiler {

  override def support(
    operator: UserOperator)(
      implicit context: OperatorCompiler.Context): Boolean = {
    (operator.annotationDesc.resolveClass == classOf[CoGroup]
      || operator.annotationDesc.resolveClass == classOf[GroupSort])
  }

  override def operatorType: OperatorType = OperatorType.CoGroupType

  override def compile(
    operator: UserOperator)(
      implicit context: OperatorCompiler.Context): Type = {

    assert(support(operator),
      s"The operator type is not supported: ${operator.annotationDesc.resolveClass.getSimpleName}"
        + s" [${operator}]")
    assert(operator.inputs.size > 0,
      s"The size of inputs should be greater than 0: ${operator.inputs.size} [${operator}]")

    assert(
      operator.methodDesc.parameterClasses
        .zip(operator.inputs.map(_ => classOf[JList[_]])
          ++: operator.outputs.map(_ => classOf[Result[_]])
          ++: operator.arguments.map(_.resolveClass))
        .forall {
          case (method, model) => method.isAssignableFrom(model)
        },
      s"The operator method parameter types are not compatible: (${
        operator.methodDesc.parameterClasses.map(_.getName).mkString("(", ",", ")")
      }, ${
        (operator.inputs.map(_ => classOf[JList[_]])
          ++: operator.outputs.map(_ => classOf[Result[_]])
          ++: operator.arguments.map(_.resolveClass)).map(_.getName).mkString("(", ",", ")")
      }) [${operator}]")

    val builder = new CoGroupOperatorFragmentClassBuilder(operator)

    context.addClass(builder)
  }
}

private class CoGroupOperatorFragmentClassBuilder(
  operator: UserOperator)(
    implicit context: OperatorCompiler.Context)
  extends UserOperatorFragmentClassBuilder(
    classOf[IndexedSeq[Iterator[_]]].asType,
    operator.implementationClass.asType,
    operator.outputs)(
    None,
    classOf[CoGroupOperatorFragment].asType) {

  val inputBuffer =
    operator.annotationDesc.getElements()("inputBuffer").resolve(context.classLoader)
      .asInstanceOf[InputBuffer]

  override def defCtor()(implicit mb: MethodBuilder): Unit = {
    val thisVar :: _ :: fragmentVars = mb.argVars

    thisVar.push().invokeInit(
      superType,
      buildIndexedSeq { builder =>
        for {
          input <- operator.inputs
        } {
          builder += pushNew0(
            inputBuffer match {
              case InputBuffer.EXPAND => classOf[ArrayListBuffer[_]].asType
              case InputBuffer.ESCAPE => classOf[FileMapListBuffer[_]].asType
            })
        }
      },
      buildIndexedSeq { builder =>
        fragmentVars.foreach {
          builder += _.push()
        }
      })
  }

  override def defMethods(methodDef: MethodDef): Unit = {
    super.defMethods(methodDef)

    methodDef.newMethod(
      "newDataModelFor",
      classOf[DataModel[_ <: DataModel[_]]].asType,
      Seq(Type.INT_TYPE),
      new MethodSignatureBuilder()
        .newFormalTypeParameter("T") {
          _.newInterfaceBound {
            _.newClassType(classOf[DataModel[_]].asType) {
              _.newTypeArgument(SignatureVisitor.INSTANCEOF, "T")
            }
          }
        }
        .newReturnType {
          _.newTypeVariable("T")
        }) { implicit mb =>
        val thisVar :: iVar :: _ = mb.argVars
        operator.inputs.zipWithIndex.foreach {
          case (input, i) =>
            iVar.push().unlessNe(ldc(i)) {
              `return`(thisVar.push().invokeV(s"newDataModelFor${i}", input.dataModelType))
            }
        }
        `throw`(pushNew0(classOf[AssertionError].asType))
      }

    operator.inputs.zipWithIndex.map {
      case (input, i) =>
        methodDef.newMethod(
          s"newDataModelFor${i}",
          input.dataModelType,
          Seq.empty) { implicit mb =>
            `return`(pushNew0(input.dataModelType))
          }
    }

    methodDef.newMethod(
      "cogroup",
      Seq(
        classOf[IndexedSeq[ListBuffer[_ <: DataModel[_]]]].asType,
        classOf[IndexedSeq[Result[_]]].asType),
      new MethodSignatureBuilder()
        .newParameterType {
          _.newClassType(classOf[IndexedSeq[_]].asType) {
            _.newTypeArgument(SignatureVisitor.INSTANCEOF) {
              _.newClassType(classOf[ListBuffer[_]].asType) {
                _.newTypeArgument(SignatureVisitor.EXTENDS) {
                  _.newClassType(classOf[DataModel[_]].asType) {
                    _.newTypeArgument()
                  }
                }
              }
            }
          }
        }
        .newParameterType {
          _.newClassType(classOf[IndexedSeq[_]].asType) {
            _.newTypeArgument(SignatureVisitor.INSTANCEOF) {
              _.newClassType(classOf[Result[_]].asType) {
                _.newTypeArgument()
              }
            }
          }
        }
        .newVoidReturnType()) { implicit mb =>
        val thisVar :: buffersVar :: outputsVar :: _ = mb.argVars

        getOperatorField()
          .invokeV(
            operator.methodDesc.getName,
            (0 until operator.inputs.size).map { i =>
              applySeq(buffersVar.push(), ldc(i)).cast(classOf[JList[_]].asType)
            }
              ++ (0 until operator.outputs.size).map { i =>
                applySeq(outputsVar.push(), ldc(i)).cast(classOf[Result[_]].asType)
              }
              ++ operator.arguments.map { argument =>
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
