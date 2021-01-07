/*
 * Copyright 2011-2021 Asakusa Framework Team.
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

import java.lang.{ Iterable => JIterable }
import java.util.{ List => JList }
import java.util.concurrent.atomic.AtomicLong

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.reflect.ClassTag

import org.objectweb.asm.Type
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.lang.compiler.analyzer.util.GroupOperatorUtil
import com.asakusafw.lang.compiler.model.graph.{ OperatorInput, UserOperator }
import com.asakusafw.runtime.core.Result
import com.asakusafw.runtime.flow.{ ArrayListBuffer, FileMapListBuffer, ListBuffer }
import com.asakusafw.runtime.core.GroupView
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.spark.compiler.spi.{ OperatorCompiler, OperatorType }
import com.asakusafw.spark.runtime.fragment.user._
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._
import com.asakusafw.spark.tools.asm4s._
import com.asakusafw.vocabulary.attribute.BufferType
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
        .zip(operator.inputs.takeWhile(_.getInputUnit != OperatorInput.InputUnit.WHOLE)
          .map(_ => classOf[JList[_]])
          ++: operator.inputs.dropWhile(_.getInputUnit != OperatorInput.InputUnit.WHOLE)
          .collect {
            case input: OperatorInput if input.getInputUnit == OperatorInput.InputUnit.WHOLE =>
              classOf[GroupView[_]]
          }
          ++: operator.outputs.map(_ => classOf[Result[_]])
          ++: operator.arguments.map(_.resolveClass))
        .forall {
          case (method, model) => method.isAssignableFrom(model)
        },
      s"The operator method parameter types are not compatible: (${
        operator.methodDesc.parameterClasses.map(_.getName).mkString("(", ",", ")")
      }, ${
        (operator.inputs.takeWhile { input =>
          input.getInputUnit != OperatorInput.InputUnit.WHOLE
        }.map(_ => classOf[JList[_]])
          ++: operator.inputs.dropWhile { input =>
            input.getInputUnit != OperatorInput.InputUnit.WHOLE
          }.collect {
            case input: OperatorInput if input.getInputUnit == OperatorInput.InputUnit.WHOLE =>
              classOf[GroupView[_]]
          }
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
    operator.inputs,
    operator.outputs)(
    None,
    classOf[CoGroupOperatorFragment].asType) {

  override def defCtor()(implicit mb: MethodBuilder): Unit = {
    val thisVar :: _ :: fragmentVars = mb.argVars

    thisVar.push().invokeInit(
      superType,
      buildIndexedSeq { builder =>
        for {
          input <- operator.inputs
        } {
          val bufferType = GroupOperatorUtil.getBufferType(input)
          builder += pushNew0(
            bufferType match {
              case BufferType.HEAP =>
                ListLikeBufferClassBuilder.getOrCompile(input.dataModelType, spill = false)
              case BufferType.SPILL =>
                ListLikeBufferClassBuilder.getOrCompile(input.dataModelType, spill = true)
              case BufferType.VOLATILE =>
                classOf[IterableBuffer[_]].asType
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
      "cogroup",
      Seq(
        classOf[IndexedSeq[JIterable[_ <: DataModel[_]]]].asType,
        classOf[IndexedSeq[Result[_]]].asType),
      new MethodSignatureBuilder()
        .newParameterType {
          _.newClassType(classOf[IndexedSeq[_]].asType) {
            _.newTypeArgument(SignatureVisitor.INSTANCEOF) {
              _.newClassType(classOf[JIterable[_]].asType) {
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
            (operator.inputs.takeWhile(_.getInputUnit != OperatorInput.InputUnit.WHOLE)
              .zipWithIndex.map {
                case (_, i) => applySeq(buffersVar.push(), ldc(i))
              }
              ++ operator.inputs.dropWhile(_.getInputUnit != OperatorInput.InputUnit.WHOLE)
              .collect {
                case input: OperatorInput if input.getInputUnit == OperatorInput.InputUnit.WHOLE =>
                  getViewField(input)
              }
              ++ (0 until operator.outputs.size).map { i =>
                applySeq(outputsVar.push(), ldc(i))
              }
              ++ operator.arguments.map { argument =>
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

private class ListLikeBufferClassBuilder(
  dataModelType: Type,
  spill: Boolean)(
    implicit context: CompilerContext)
  extends ClassBuilder(
    Type.getType(
      s"L${GeneratedClassPackageInternalName}/${context.flowId}/fragment/ListLikeBuffer$$${ListLikeBufferClassBuilder.nextId};"), // scalastyle:ignore
    new ClassSignatureBuilder()
      .newSuperclass {
        _.newClassType(classOf[ListLikeBuffer[_]].asType) {
          _.newTypeArgument(SignatureVisitor.INSTANCEOF, dataModelType)
        }
      },
    classOf[ListLikeBuffer[_]].asType) {

  override def defConstructors(ctorDef: ConstructorDef): Unit = {
    ctorDef.newInit(Seq.empty) { implicit mb =>
      val thisVar :: _ = mb.argVars
      thisVar.push().invokeInit(
        superType,
        pushNew0(
          if (spill) {
            classOf[FileMapListBuffer[_]].asType
          } else {
            classOf[ArrayListBuffer[_]].asType
          }).asType(classOf[ListBuffer[_]].asType))
    }
  }

  override def defMethods(methodDef: MethodDef): Unit = {
    super.defMethods(methodDef)

    methodDef.newMethod("newDataModel", classOf[DataModel[_]].asType, Seq.empty) { implicit mb =>
      val thisVar :: _ = mb.argVars
      `return`(thisVar.push().invokeV("newDataModel", dataModelType))
    }

    methodDef.newMethod("newDataModel", dataModelType, Seq.empty) { implicit mb =>
      `return`(pushNew0(dataModelType))
    }
  }
}

private object ListLikeBufferClassBuilder {

  private[this] val curIds: mutable.Map[CompilerContext, AtomicLong] =
    mutable.WeakHashMap.empty

  def nextId(implicit context: CompilerContext): Long =
    curIds.getOrElseUpdate(context, new AtomicLong(0L)).getAndIncrement()

  private[this] val cache: mutable.Map[CompilerContext, mutable.Map[(Type, Boolean), Type]] =
    mutable.WeakHashMap.empty

  def getOrCompile(
    dataModelType: Type, spill: Boolean)(
      implicit context: CompilerContext): Type = {
    cache.getOrElseUpdate(context, mutable.Map.empty)
      .getOrElseUpdate(
        (dataModelType, spill),
        context.addClass(new ListLikeBufferClassBuilder(dataModelType, spill)))
  }
}
