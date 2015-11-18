/*
 * Copyright 2011-2015 Asakusa Framework Team.
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

import com.asakusafw.lang.compiler.analyzer.util.JoinedModelUtil
import com.asakusafw.lang.compiler.model.graph.UserOperator
import com.asakusafw.spark.compiler.spi.{ OperatorCompiler, OperatorType }
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
    operator.outputs) {

  val mappings =
    JoinedModelUtil.getPropertyMappings(context.classLoader, operator).toSeq

  override def defFields(fieldDef: FieldDef): Unit = {
    super.defFields(fieldDef)
    fieldDef.newField("leftDataModel", operator.outputs(Split.ID_OUTPUT_LEFT).dataModelType)
    fieldDef.newField("rightDataModel", operator.outputs(Split.ID_OUTPUT_RIGHT).dataModelType)
  }

  override def initFields(mb: MethodBuilder): Unit = {
    super.initFields(mb)

    import mb._ // scalastyle:ignore
    thisVar.push().putField(
      "leftDataModel",
      operator.outputs(Split.ID_OUTPUT_LEFT).dataModelType,
      pushNew0(operator.outputs(Split.ID_OUTPUT_LEFT).dataModelType))
    thisVar.push().putField(
      "rightDataModel",
      operator.outputs(Split.ID_OUTPUT_RIGHT).dataModelType,
      pushNew0(operator.outputs(Split.ID_OUTPUT_RIGHT).dataModelType))
  }

  override def defAddMethod(mb: MethodBuilder, dataModelVar: Var): Unit = {
    import mb._ // scalastyle:ignore

    val leftVar = thisVar.push()
      .getField("leftDataModel", operator.outputs(Split.ID_OUTPUT_LEFT).dataModelType)
      .store(dataModelVar.nextLocal)
    val rightVar = thisVar.push()
      .getField("rightDataModel", operator.outputs(Split.ID_OUTPUT_RIGHT).dataModelType)
      .store(leftVar.nextLocal)
    leftVar.push().invokeV("reset")
    rightVar.push().invokeV("reset")

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

      pushObject(mb)(ValueOptionOps)
        .invokeV(
          "copy",
          dataModelVar.push()
            .invokeV(srcProperty.getDeclaration.getName, srcProperty.getType.asType),
          destVar.push()
            .invokeV(destProperty.getDeclaration.getName, destProperty.getType.asType))
    }

    getOutputField(mb, operator.outputs(Split.ID_OUTPUT_LEFT))
      .invokeV("add", leftVar.push().asType(classOf[AnyRef].asType))
    getOutputField(mb, operator.outputs(Split.ID_OUTPUT_RIGHT))
      .invokeV("add", rightVar.push().asType(classOf[AnyRef].asType))

    `return`()
  }
}
