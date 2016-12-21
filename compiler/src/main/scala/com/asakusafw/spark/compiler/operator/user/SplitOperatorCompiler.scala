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

import scala.collection.JavaConversions._

import org.objectweb.asm.Type
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.lang.compiler.analyzer.util.JoinedModelUtil
import com.asakusafw.lang.compiler.model.graph.UserOperator
import com.asakusafw.runtime.core.Result
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.spark.compiler.spi.{ OperatorCompiler, OperatorType }
import com.asakusafw.spark.runtime.fragment.user.SplitOperatorFragment
import com.asakusafw.spark.runtime.util.ValueOptionOps
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._
import com.asakusafw.spark.tools.asm4s._
import com.asakusafw.vocabulary.operator.Split

class SplitOperatorCompiler extends UserOperatorCompiler {

  override def support(
    operator: UserOperator)(
      implicit context: OperatorCompiler.Context): Boolean = {
    operator.annotationDesc.resolveClass == classOf[Split]
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
    assert(operator.outputs.size == 2,
      s"The size of outputs should be 2: ${operator.outputs.size} [${operator}]")

    val builder = new SplitOperatorFragmentClassBuilder(operator)

    context.addClass(builder)
  }
}

private class SplitOperatorFragmentClassBuilder(
  operator: UserOperator)(
    implicit context: OperatorCompiler.Context)
  extends UserOperatorFragmentClassBuilder(
    operator.inputs(Split.ID_INPUT).dataModelType,
    operator.implementationClass.asType,
    operator.inputs,
    operator.outputs)(
    Option(
      new ClassSignatureBuilder()
        .newSuperclass {
          _.newClassType(classOf[SplitOperatorFragment[_, _, _]].asType) {
            _.newTypeArgument(
              SignatureVisitor.INSTANCEOF,
              operator.inputs(Split.ID_INPUT).dataModelType)
              .newTypeArgument(
                SignatureVisitor.INSTANCEOF,
                operator.outputs(Split.ID_OUTPUT_LEFT).dataModelType)
              .newTypeArgument(
                SignatureVisitor.INSTANCEOF,
                operator.outputs(Split.ID_OUTPUT_RIGHT).dataModelType)
          }
        }),
    classOf[SplitOperatorFragment[_, _, _]].asType) {

  val mappings =
    JoinedModelUtil.getPropertyMappings(context.classLoader, operator).toSeq

  override def defCtor()(implicit mb: MethodBuilder): Unit = {
    val thisVar :: _ :: fragmentVars = mb.argVars

    thisVar.push().invokeInit(
      superType,
      pushNew0(operator.outputs(Split.ID_OUTPUT_LEFT).dataModelType)
        .asType(classOf[DataModel[_]].asType),
      pushNew0(operator.outputs(Split.ID_OUTPUT_RIGHT).dataModelType)
        .asType(classOf[DataModel[_]].asType),
      fragmentVars(Split.ID_OUTPUT_LEFT).push(),
      fragmentVars(Split.ID_OUTPUT_RIGHT).push())
  }

  override def defMethods(methodDef: MethodDef): Unit = {
    super.defMethods(methodDef)

    methodDef.newMethod(
      "split",
      Seq(
        classOf[DataModel[_]].asType,
        classOf[DataModel[_]].asType,
        classOf[DataModel[_]].asType),
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
        .newParameterType {
          _.newClassType(classOf[DataModel[_]].asType) {
            _.newTypeArgument()
          }
        }
        .newVoidReturnType()) { implicit mb =>
        val thisVar :: inputVar :: leftVar :: rightVar :: _ = mb.argVars
        thisVar.push().invokeV(
          "split",
          inputVar.push().cast(dataModelType),
          leftVar.push().cast(operator.outputs(Split.ID_OUTPUT_LEFT).dataModelType),
          rightVar.push().cast(operator.outputs(Split.ID_OUTPUT_RIGHT).dataModelType))
        `return`()
      }

    methodDef.newMethod(
      "split",
      Seq(
        dataModelType,
        operator.outputs(Split.ID_OUTPUT_LEFT).dataModelType,
        operator.outputs(Split.ID_OUTPUT_RIGHT).dataModelType)) { implicit mb =>
        val thisVar :: inputVar :: leftVar :: rightVar :: _ = mb.argVars

        val vars = Seq(leftVar, rightVar)

        mappings.foreach { mapping =>
          assert(mapping.getSourcePort == operator.inputs(Split.ID_INPUT),
            "The source port should be the same as the port for Split.ID_INPUT: " +
              s"(${mapping.getSourcePort}, ${operator.inputs(Split.ID_INPUT)}) [${operator}]")
          val srcProperty =
            operator.inputs(Split.ID_INPUT).dataModelRef.findProperty(mapping.getSourceProperty)

          val dest = operator.outputs.indexOf(mapping.getDestinationPort)
          val destVar = vars(dest)
          val destProperty =
            operator.outputs(dest).dataModelRef.findProperty(mapping.getDestinationProperty)

          assert(srcProperty.getType.asType == destProperty.getType.asType,
            "The source and destination types should be the same: "
              + s"(${srcProperty.getType}, ${destProperty.getType}) [${operator}]")

          pushObject(ValueOptionOps)
            .invokeV(
              "copy",
              inputVar.push()
                .invokeV(srcProperty.getDeclaration.getName, srcProperty.getType.asType),
              destVar.push()
                .invokeV(destProperty.getDeclaration.getName, destProperty.getType.asType))
        }

        `return`()
      }
  }
}
