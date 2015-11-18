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
package aggregation

import scala.collection.JavaConversions._

import org.objectweb.asm.Type

import com.asakusafw.lang.compiler.analyzer.util.{ PropertyFolding, SummarizedModelUtil }
import com.asakusafw.lang.compiler.model.graph.UserOperator
import com.asakusafw.spark.compiler.spi.AggregationCompiler
import com.asakusafw.spark.runtime.util.ValueOptionOps
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._
import com.asakusafw.spark.tools.asm4s._
import com.asakusafw.vocabulary.flow.processor.PartialAggregation
import com.asakusafw.vocabulary.operator.Summarize

class SummarizeAggregationCompiler extends AggregationCompiler {

  def of: Class[_] = classOf[Summarize]

  def compile(
    operator: UserOperator)(
      implicit context: AggregationCompiler.Context): Type = {

    assert(operator.annotationDesc.resolveClass == of,
      s"The operator type is not supported: ${operator.annotationDesc.resolveClass.getSimpleName}"
        + s" [${operator}]")
    assert(operator.inputs.size == 1,
      s"The size of inputs should be 1: ${operator.inputs.size} [${operator}]")
    assert(operator.outputs.size == 1,
      s"The size of outputs should be 1: ${operator.outputs.size} [${operator}]")

    val builder = new SummarizeAggregationClassBuilder(operator)

    context.addClass(builder)
  }
}

private class SummarizeAggregationClassBuilder(
  operator: UserOperator)(
    implicit context: AggregationCompiler.Context)
  extends AggregationClassBuilder(
    operator.inputs(Summarize.ID_INPUT).dataModelType,
    operator.outputs(Summarize.ID_OUTPUT).dataModelType) {

  val propertyFoldings =
    SummarizedModelUtil.getPropertyFoldings(context.classLoader, operator).toSeq

  override def defMapSideCombine(mb: MethodBuilder): Unit = {
    import mb._ // scalastyle:ignore
    val partialAggregation =
      operator.annotationDesc.elements("partialAggregation")
        .value.asInstanceOf[PartialAggregation]
    `return`(ldc(partialAggregation != PartialAggregation.TOTAL))
  }

  override def defNewCombiner(mb: MethodBuilder): Unit = {
    import mb._ // scalastyle:ignore
    `return`(pushNew0(combinerType))
  }

  override def defInitCombinerByValue(
    mb: MethodBuilder, combinerVar: Var, valueVar: Var): Unit = {
    import mb._ // scalastyle:ignore
    propertyFoldings.foreach { folding =>
      val mapping = folding.getMapping
      val valuePropertyRef =
        operator.inputs(Summarize.ID_INPUT)
          .dataModelRef.findProperty(mapping.getSourceProperty)
      val combinerPropertyRef =
        operator.outputs(Summarize.ID_OUTPUT)
          .dataModelRef.findProperty(mapping.getDestinationProperty)
      folding.getAggregation match {
        case PropertyFolding.Aggregation.ANY =>
          pushObject(mb)(ValueOptionOps)
            .invokeV("copy",
              valueVar.push().invokeV(
                valuePropertyRef.getDeclaration.getName,
                valuePropertyRef.getType.asType),
              combinerVar.push().invokeV(
                combinerPropertyRef.getDeclaration.getName,
                combinerPropertyRef.getType.asType))

        case PropertyFolding.Aggregation.SUM =>
          pushObject(mb)(ValueOptionOps)
            .invokeV("setZero",
              combinerVar.push().invokeV(
                combinerPropertyRef.getDeclaration.getName,
                combinerPropertyRef.getType.asType))

        case PropertyFolding.Aggregation.COUNT =>
          pushObject(mb)(ValueOptionOps)
            .invokeV("setZero",
              combinerVar.push().invokeV(
                combinerPropertyRef.getDeclaration.getName,
                combinerPropertyRef.getType.asType))

        case _ => // NoOP
      }
    }
    `return`(
      thisVar.push().invokeV("mergeValue", combinerType, combinerVar.push(), valueVar.push()))
  }

  override def defMergeValue(
    mb: MethodBuilder, combinerVar: Var, valueVar: Var): Unit = {
    import mb._ // scalastyle:ignore
    propertyFoldings.foreach { folding =>
      val mapping = folding.getMapping
      val valuePropertyRef =
        operator.inputs(Summarize.ID_INPUT)
          .dataModelRef.findProperty(mapping.getSourceProperty)
      val combinerPropertyRef =
        operator.outputs(Summarize.ID_OUTPUT)
          .dataModelRef.findProperty(mapping.getDestinationProperty)
      folding.getAggregation match {
        case PropertyFolding.Aggregation.SUM =>
          pushObject(mb)(ValueOptionOps)
            .invokeV("add",
              valueVar.push().invokeV(
                valuePropertyRef.getDeclaration.getName,
                valuePropertyRef.getType.asType),
              combinerVar.push().invokeV(
                combinerPropertyRef.getDeclaration.getName,
                combinerPropertyRef.getType.asType))

        case PropertyFolding.Aggregation.MAX =>
          pushObject(mb)(ValueOptionOps)
            .invokeV("max",
              combinerVar.push().invokeV(
                combinerPropertyRef.getDeclaration.getName,
                combinerPropertyRef.getType.asType),
              valueVar.push().invokeV(
                valuePropertyRef.getDeclaration.getName,
                valuePropertyRef.getType.asType))

        case PropertyFolding.Aggregation.MIN =>
          pushObject(mb)(ValueOptionOps)
            .invokeV("min",
              combinerVar.push().invokeV(
                combinerPropertyRef.getDeclaration.getName,
                combinerPropertyRef.getType.asType),
              valueVar.push().invokeV(
                valuePropertyRef.getDeclaration.getName,
                valuePropertyRef.getType.asType))

        case PropertyFolding.Aggregation.COUNT =>
          pushObject(mb)(ValueOptionOps)
            .invokeV("inc",
              combinerVar.push().invokeV(
                combinerPropertyRef.getDeclaration.getName,
                combinerPropertyRef.getType.asType))

        case _ => // NoOP
      }
    }
    `return`(combinerVar.push())
  }

  override def defInitCombinerByCombiner(
    mb: MethodBuilder, comb1Var: Var, comb2Var: Var): Unit = {
    import mb._ // scalastyle:ignore
    comb1Var.push().invokeV("copyFrom", comb2Var.push())
    `return`(comb1Var.push())
  }

  override def defMergeCombiners(
    mb: MethodBuilder, comb1Var: Var, comb2Var: Var): Unit = {
    import mb._ // scalastyle:ignore
    propertyFoldings.foreach { folding =>
      val mapping = folding.getMapping
      val combinerPropertyRef =
        operator.outputs(Summarize.ID_OUTPUT)
          .dataModelRef.findProperty(mapping.getDestinationProperty)
      folding.getAggregation match {
        case PropertyFolding.Aggregation.SUM =>
          pushObject(mb)(ValueOptionOps)
            .invokeV("add",
              comb2Var.push().invokeV(
                combinerPropertyRef.getDeclaration.getName,
                combinerPropertyRef.getType.asType),
              comb1Var.push().invokeV(
                combinerPropertyRef.getDeclaration.getName,
                combinerPropertyRef.getType.asType))

        case PropertyFolding.Aggregation.MAX =>
          pushObject(mb)(ValueOptionOps)
            .invokeV("max",
              comb1Var.push().invokeV(
                combinerPropertyRef.getDeclaration.getName,
                combinerPropertyRef.getType.asType),
              comb2Var.push().invokeV(
                combinerPropertyRef.getDeclaration.getName,
                combinerPropertyRef.getType.asType))

        case PropertyFolding.Aggregation.MIN =>
          pushObject(mb)(ValueOptionOps)
            .invokeV("min",
              comb1Var.push().invokeV(
                combinerPropertyRef.getDeclaration.getName,
                combinerPropertyRef.getType.asType),
              comb2Var.push().invokeV(
                combinerPropertyRef.getDeclaration.getName,
                combinerPropertyRef.getType.asType))

        case PropertyFolding.Aggregation.COUNT =>
          pushObject(mb)(ValueOptionOps)
            .invokeV("add",
              comb2Var.push().invokeV(
                combinerPropertyRef.getDeclaration.getName,
                combinerPropertyRef.getType.asType),
              comb1Var.push().invokeV(
                combinerPropertyRef.getDeclaration.getName,
                combinerPropertyRef.getType.asType))

        case _ => // NoOP
      }
    }
    `return`(comb1Var.push())
  }
}
