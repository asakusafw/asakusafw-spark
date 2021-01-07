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
package aggregation

import scala.collection.JavaConversions._
import org.apache.spark.broadcast.{ Broadcast => Broadcasted }
import org.objectweb.asm.Type
import org.objectweb.asm.signature.SignatureVisitor
import com.asakusafw.lang.compiler.analyzer.util.{ PropertyFolding, SummarizedModelUtil }
import com.asakusafw.lang.compiler.model.graph.UserOperator
import com.asakusafw.runtime.value.ValueOption
import com.asakusafw.spark.compiler.spi.AggregationCompiler
import com.asakusafw.spark.runtime.graph.BroadcastId
import com.asakusafw.spark.runtime.util.ValueOptionOps
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._
import com.asakusafw.spark.tools.asm4s._
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
    operator.outputs(Summarize.ID_OUTPUT).dataModelType)(
    AggregationClassBuilder.AggregationType.Summarize) {

  val propertyFoldings =
    SummarizedModelUtil.getPropertyFoldings(context.classLoader, operator).toSeq

  override def defConstructors(ctorDef: ConstructorDef): Unit = {
    ctorDef.newInit(
      Seq(classOf[Map[BroadcastId, Broadcasted[_]]].asType),
      new MethodSignatureBuilder()
        .newParameterType {
          _.newClassType(classOf[Map[_, _]].asType) {
            _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[BroadcastId].asType)
              .newTypeArgument(SignatureVisitor.INSTANCEOF) {
                _.newClassType(classOf[Broadcasted[_]].asType) {
                  _.newTypeArgument()
                }
              }
          }
        }
        .newVoidReturnType()) { implicit mb =>
        val thisVar :: _ = mb.argVars
        thisVar.push().invokeInit(superType)
      }
  }

  override def defNewCombiner()(implicit mb: MethodBuilder): Unit = {
    `return`(pushNew0(combinerType))
  }

  override def defInitCombinerByValue(
    combinerVar: Var, valueVar: Var)(
      implicit mb: MethodBuilder): Unit = {
    val thisVar :: _ = mb.argVars

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
          pushObject(ValueOptionOps)
            .invokeV("copyAlways",
              combinerVar.push().invokeV(
                combinerPropertyRef.getDeclaration.getName,
                combinerPropertyRef.getType.asType).asType(classOf[ValueOption[_]].asType),
              valueVar.push().invokeV(
                valuePropertyRef.getDeclaration.getName,
                valuePropertyRef.getType.asType).asType(classOf[ValueOption[_]].asType))
          // 'any' merges nothing

        case PropertyFolding.Aggregation.MAX | PropertyFolding.Aggregation.MIN =>
          pushObject(ValueOptionOps)
            .invokeV("copyWithName",
              combinerVar.push().invokeV(
                combinerPropertyRef.getDeclaration.getName,
                combinerPropertyRef.getType.asType).asType(classOf[ValueOption[_]].asType),
              valueVar.push().invokeV(
                valuePropertyRef.getDeclaration.getName,
                valuePropertyRef.getType.asType).asType(classOf[ValueOption[_]].asType),
              ldc(getOperandName(folding)),
              valueVar.push().asType(classOf[AnyRef].asType))
          // always {max,min}(a, a) = a

        case PropertyFolding.Aggregation.SUM =>
          pushObject(ValueOptionOps)
            .invokeV("setZero",
              combinerVar.push().invokeV(
                combinerPropertyRef.getDeclaration.getName,
                combinerPropertyRef.getType.asType))
          generateMergeValueStatement(folding, combinerVar, valueVar)

        case PropertyFolding.Aggregation.COUNT =>
          pushObject(ValueOptionOps)
            .invokeV("setZero",
              combinerVar.push().invokeV(
                combinerPropertyRef.getDeclaration.getName,
                combinerPropertyRef.getType.asType))
          generateMergeValueStatement(folding, combinerVar, valueVar)

        case _ => // NoOP
      }
    }
    `return`(combinerVar.push())
  }

  override def defMergeValue(
    combinerVar: Var, valueVar: Var)(
      implicit mb: MethodBuilder): Unit = {
    propertyFoldings.foreach { folding =>
      generateMergeValueStatement(folding, combinerVar, valueVar)
    }
    `return`(combinerVar.push())
  }

  private[this] def getOperandName(folding: PropertyFolding): String = {
    val property =
      operator.inputs(Summarize.ID_INPUT)
        .dataModelRef.findProperty(folding.getMapping.getSourceProperty)
    "@%s:%s.%s(%s.%s)".format(
      operator.getAnnotation.getDeclaringClass.getSimpleName,
      operator.getMethod.getDeclaringClass.getClassName,
      operator.getMethod.getName,
      property.getOwner.getDeclaration.getSimpleName,
      property.getName.toName)
  }

  private[this] def generateMergeValueStatement(
    folding: PropertyFolding, combinerVar: Var, valueVar: Var)(
      implicit mb: MethodBuilder): Unit = {
    val mapping = folding.getMapping
    val valuePropertyRef =
      operator.inputs(Summarize.ID_INPUT)
        .dataModelRef.findProperty(mapping.getSourceProperty)
    val combinerPropertyRef =
      operator.outputs(Summarize.ID_OUTPUT)
        .dataModelRef.findProperty(mapping.getDestinationProperty)
    folding.getAggregation match {
      case PropertyFolding.Aggregation.SUM =>
        pushObject(ValueOptionOps)
          .invokeV("addWithName",
            combinerVar.push().invokeV(
              combinerPropertyRef.getDeclaration.getName,
              combinerPropertyRef.getType.asType),
            valueVar.push().invokeV(
              valuePropertyRef.getDeclaration.getName,
              valuePropertyRef.getType.asType),
            ldc(getOperandName(folding)),
            valueVar.push().asType(classOf[AnyRef].asType))

      case PropertyFolding.Aggregation.MAX =>
        pushObject(ValueOptionOps)
          .invokeV("maxWithName",
            combinerVar.push().invokeV(
              combinerPropertyRef.getDeclaration.getName,
              combinerPropertyRef.getType.asType).asType(classOf[ValueOption[_]].asType),
            valueVar.push().invokeV(
              valuePropertyRef.getDeclaration.getName,
              valuePropertyRef.getType.asType).asType(classOf[ValueOption[_]].asType),
            ldc(getOperandName(folding)),
            valueVar.push().asType(classOf[AnyRef].asType))

      case PropertyFolding.Aggregation.MIN =>
        pushObject(ValueOptionOps)
          .invokeV("minWithName",
            combinerVar.push().invokeV(
              combinerPropertyRef.getDeclaration.getName,
              combinerPropertyRef.getType.asType).asType(classOf[ValueOption[_]].asType),
            valueVar.push().invokeV(
              valuePropertyRef.getDeclaration.getName,
              valuePropertyRef.getType.asType).asType(classOf[ValueOption[_]].asType),
            ldc(getOperandName(folding)),
            valueVar.push().asType(classOf[AnyRef].asType))

      case PropertyFolding.Aggregation.COUNT =>
        pushObject(ValueOptionOps)
          .invokeV("inc",
            combinerVar.push().invokeV(
              combinerPropertyRef.getDeclaration.getName,
              combinerPropertyRef.getType.asType))

      case _ => // NoOP
    }
  }

  override def defInitCombinerByCombiner(
    comb1Var: Var, comb2Var: Var)(implicit mb: MethodBuilder): Unit = {
    comb1Var.push().invokeV("copyFrom", comb2Var.push())
    `return`(comb1Var.push())
  }

  override def defMergeCombiners(
    comb1Var: Var, comb2Var: Var)(implicit mb: MethodBuilder): Unit = {
    propertyFoldings.foreach { folding =>
      val mapping = folding.getMapping
      val combinerPropertyRef =
        operator.outputs(Summarize.ID_OUTPUT)
          .dataModelRef.findProperty(mapping.getDestinationProperty)
      folding.getAggregation match {
        case PropertyFolding.Aggregation.SUM | PropertyFolding.Aggregation.COUNT =>
          pushObject(ValueOptionOps)
            .invokeV("addUnsafe",
              comb1Var.push().invokeV(
                combinerPropertyRef.getDeclaration.getName,
                combinerPropertyRef.getType.asType),
              comb2Var.push().invokeV(
                combinerPropertyRef.getDeclaration.getName,
                combinerPropertyRef.getType.asType))

        case PropertyFolding.Aggregation.MAX =>
          pushObject(ValueOptionOps)
            .invokeV("maxUnsafe",
              comb1Var.push().invokeV(
                combinerPropertyRef.getDeclaration.getName,
                combinerPropertyRef.getType.asType).asType(classOf[ValueOption[_]].asType),
              comb2Var.push().invokeV(
                combinerPropertyRef.getDeclaration.getName,
                combinerPropertyRef.getType.asType).asType(classOf[ValueOption[_]].asType))

        case PropertyFolding.Aggregation.MIN =>
          pushObject(ValueOptionOps)
            .invokeV("minUnsafe",
              comb1Var.push().invokeV(
                combinerPropertyRef.getDeclaration.getName,
                combinerPropertyRef.getType.asType).asType(classOf[ValueOption[_]].asType),
              comb2Var.push().invokeV(
                combinerPropertyRef.getDeclaration.getName,
                combinerPropertyRef.getType.asType).asType(classOf[ValueOption[_]].asType))

        case _ => // NoOP
      }
    }
    `return`(comb1Var.push())
  }
}
