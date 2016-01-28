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

import org.objectweb.asm.{ Opcodes, Type }

import com.asakusafw.lang.compiler.model.graph.UserOperator
import com.asakusafw.runtime.core.Result
import com.asakusafw.runtime.flow.{ ArrayListBuffer, FileMapListBuffer, ListBuffer }
import com.asakusafw.spark.compiler.spi.{ OperatorCompiler, OperatorType }
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
    operator.outputs) {

  val inputBuffer =
    operator.annotationDesc.getElements()("inputBuffer").resolve(context.classLoader)
      .asInstanceOf[InputBuffer]

  override def defFields(fieldDef: FieldDef): Unit = {
    super.defFields(fieldDef)

    fieldDef.newField(
      Opcodes.ACC_PRIVATE | Opcodes.ACC_FINAL,
      "buffers",
      classOf[Array[ListBuffer[_]]].asType,
      new TypeSignatureBuilder()
        .newArrayType {
          _.newClassType(classOf[ListBuffer[_]].asType) {
            _.newTypeArgument()
          }
        })
  }

  override def initFields()(implicit mb: MethodBuilder): Unit = {
    super.initFields()

    val thisVar :: _ = mb.argVars

    thisVar.push().putField("buffers",
      buildArray(classOf[ListBuffer[_]].asType) { builder =>
        for {
          input <- operator.inputs
        } {
          builder += pushNew0(
            inputBuffer match {
              case InputBuffer.EXPAND => classOf[ArrayListBuffer[_]].asType
              case InputBuffer.ESCAPE => classOf[FileMapListBuffer[_]].asType
            })
        }
      })
  }

  override def defAddMethod(dataModelVar: Var)(implicit mb: MethodBuilder): Unit = {
    val thisVar :: _ = mb.argVars

    val bufferVars = operator.inputs.zipWithIndex.map {
      case (input, i) =>
        val iterVar = applySeq(dataModelVar.push(), ldc(i))
          .cast(classOf[Iterator[_]].asType)
          .store()
        val bufferVar = thisVar.push()
          .getField("buffers", classOf[Array[ListBuffer[_]]].asType)
          .aload(ldc(i))
          .store()
        bufferVar.push().invokeI("begin")

        whileLoop(iterVar.push().invokeI("hasNext", Type.BOOLEAN_TYPE)) { ctrl =>
          bufferVar.push().invokeI("isExpandRequired", Type.BOOLEAN_TYPE).unlessFalse {
            bufferVar.push().invokeI(
              "expand", pushNew0(input.dataModelType).asType(classOf[AnyRef].asType))
          }
          bufferVar.push().invokeI("advance", classOf[AnyRef].asType)
            .cast(input.dataModelType)
            .invokeV(
              "copyFrom",
              iterVar.push().invokeI("next", classOf[AnyRef].asType)
                .cast(input.dataModelType))
        }

        bufferVar.push().invokeI("end")
        bufferVar
    }

    getOperatorField()
      .invokeV(
        operator.methodDesc.getName,
        bufferVars.map(_.push().asType(classOf[JList[_]].asType))
          ++ operator.outputs.map { output =>
            getOutputField(output).asType(classOf[Result[_]].asType)
          }
          ++ operator.arguments.map { argument =>
            Option(argument.value).map { value =>
              ldc(value)(ClassTag(argument.resolveClass), implicitly)
            }.getOrElse {
              pushNull(argument.resolveClass.asType)
            }
          }: _*)

    val iVar = ldc(0).store()
    loop { ctrl =>
      iVar.push().unlessLessThan(ldc(operator.inputs.size))(ctrl.break())
      thisVar.push()
        .getField("buffers", classOf[Array[ListBuffer[_]]].asType)
        .aload(iVar.push())
        .invokeI("shrink")
      iVar.inc(1)
    }

    `return`()
  }
}
