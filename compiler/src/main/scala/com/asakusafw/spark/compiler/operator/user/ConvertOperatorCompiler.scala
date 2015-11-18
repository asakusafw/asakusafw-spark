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

import scala.reflect.ClassTag

import org.objectweb.asm.Type

import com.asakusafw.lang.compiler.model.graph.UserOperator
import com.asakusafw.spark.compiler.spi.{ OperatorCompiler, OperatorType }
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
    assert(operator.inputs.size == 1, // FIXME to take multiple inputs for side data?
      s"The size of inputs should be 1: ${operator.inputs.size} [${operator}]")
    assert(operator.outputs.size == 2,
      s"The size of outputs should be 2: ${operator.outputs.size} [${operator}]")

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
    operator.outputs) {

  override def defAddMethod(dataModelVar: Var)(implicit mb: MethodBuilder): Unit = {
    import mb._ // scalastyle:ignore
    getOutputField(operator.outputs(Convert.ID_OUTPUT_ORIGINAL))
      .invokeV("add", dataModelVar.push().asType(classOf[AnyRef].asType))
    getOutputField(operator.outputs(Convert.ID_OUTPUT_CONVERTED))
      .invokeV("add",
        getOperatorField()
          .invokeV(
            operator.methodDesc.getName,
            operator.outputs(Convert.ID_OUTPUT_CONVERTED).dataModelType,
            dataModelVar.push().asType(operator.methodDesc.asType.getArgumentTypes()(0))
              +: operator.arguments.map { argument =>
                ldc(argument.value)(ClassTag(argument.resolveClass))
              }: _*)
          .asType(classOf[AnyRef].asType))
    `return`()
  }
}
