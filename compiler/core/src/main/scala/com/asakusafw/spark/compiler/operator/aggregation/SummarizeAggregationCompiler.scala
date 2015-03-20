package com.asakusafw.spark.compiler
package operator
package aggregation

import java.math.{ BigDecimal => JBigDecimal }

import scala.collection.JavaConversions._
import scala.reflect.ClassTag

import org.objectweb.asm.Type

import com.asakusafw.lang.compiler.analyzer.util.{ PropertyFolding, SummarizedModelUtil }
import com.asakusafw.lang.compiler.model.graph._
import com.asakusafw.spark.compiler.spi.AggregationCompiler
import com.asakusafw.spark.runtime.util.ValueOptionOps
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._
import com.asakusafw.vocabulary.flow.processor.PartialAggregation
import com.asakusafw.vocabulary.operator.Summarize

class SummarizeAggregationCompiler extends AggregationCompiler {

  def of: Class[_] = classOf[Summarize]

  def compile(operator: UserOperator)(implicit context: Context): Type = {
    val annotationDesc = operator.getAnnotation
    assert(annotationDesc.getDeclaringClass.resolve(context.jpContext.getClassLoader) == of)

    val inputs = operator.getInputs.toSeq
    assert(inputs.size == 1)
    val input = inputs.head
    val inputDataModelRef = context.jpContext.getDataModelLoader.load(input.getDataType)
    val inputDataModelType = inputDataModelRef.getDeclaration.asType

    val outputs = operator.getOutputs.toSeq
    assert(outputs.size == 1)
    val output = outputs.head
    val outputDataModelRef = context.jpContext.getDataModelLoader.load(output.getDataType)
    val outputDataModelType = outputDataModelRef.getDeclaration.asType

    val propertyFoldings = SummarizedModelUtil.getPropertyFoldings(context.jpContext.getClassLoader, operator).toSeq

    val builder = new AggregationClassBuilder(
      context.flowId, classOf[Seq[_]].asType, inputDataModelType, outputDataModelType) {

      override def defMapSideCombiner(mb: MethodBuilder): Unit = {
        import mb._
        val partialAggregation = annotationDesc.getElements()("partialAggregation")
          .resolve(context.jpContext.getClassLoader).asInstanceOf[PartialAggregation]
        `return`(ldc(partialAggregation != PartialAggregation.TOTAL))
      }

      override def defCreateCombiner(mb: MethodBuilder, valueVar: Var): Unit = {
        import mb._
        val combinerVar = pushNew0(combinerType).store(valueVar.nextLocal)
        propertyFoldings.foreach { folding =>
          val mapping = folding.getMapping
          val valuePropertyRef = inputDataModelRef.findProperty(mapping.getSourceProperty)
          val combinerPropertyRef = outputDataModelRef.findProperty(mapping.getDestinationProperty)
          folding.getAggregation match {
            case PropertyFolding.Aggregation.ANY =>
              getStatic(ValueOptionOps.getClass.asType, "MODULE$", ValueOptionOps.getClass.asType)
                .invokeV("copy",
                  valueVar.push().invokeV(valuePropertyRef.getDeclaration.getName, valuePropertyRef.getType.asType),
                  combinerVar.push().invokeV(combinerPropertyRef.getDeclaration.getName, combinerPropertyRef.getType.asType))

            case PropertyFolding.Aggregation.SUM =>
              getStatic(ValueOptionOps.getClass.asType, "MODULE$", ValueOptionOps.getClass.asType)
                .invokeV("setZero",
                  combinerVar.push().invokeV(combinerPropertyRef.getDeclaration.getName, combinerPropertyRef.getType.asType))

            case PropertyFolding.Aggregation.COUNT =>
              getStatic(ValueOptionOps.getClass.asType, "MODULE$", ValueOptionOps.getClass.asType)
                .invokeV("setZero",
                  combinerVar.push().invokeV(combinerPropertyRef.getDeclaration.getName, combinerPropertyRef.getType.asType))

            case _ => // NoOP
          }
        }
        thisVar.push().invokeV("mergeValue", combinerType, combinerVar.push(), valueVar.push()).pop()
        `return`(combinerVar.push())
      }

      override def defMergeValue(mb: MethodBuilder, combinerVar: Var, valueVar: Var): Unit = {
        import mb._
        propertyFoldings.foreach { folding =>
          val mapping = folding.getMapping
          val valuePropertyRef = inputDataModelRef.findProperty(mapping.getSourceProperty)
          val combinerPropertyRef = outputDataModelRef.findProperty(mapping.getDestinationProperty)
          folding.getAggregation match {
            case PropertyFolding.Aggregation.SUM =>
              getStatic(ValueOptionOps.getClass.asType, "MODULE$", ValueOptionOps.getClass.asType)
                .invokeV("add",
                  valueVar.push().invokeV(valuePropertyRef.getDeclaration.getName, valuePropertyRef.getType.asType),
                  combinerVar.push().invokeV(combinerPropertyRef.getDeclaration.getName, combinerPropertyRef.getType.asType))

            case PropertyFolding.Aggregation.MAX =>
              getStatic(ValueOptionOps.getClass.asType, "MODULE$", ValueOptionOps.getClass.asType)
                .invokeV("max",
                  combinerVar.push().invokeV(combinerPropertyRef.getDeclaration.getName, combinerPropertyRef.getType.asType),
                  valueVar.push().invokeV(valuePropertyRef.getDeclaration.getName, valuePropertyRef.getType.asType))

            case PropertyFolding.Aggregation.MIN =>
              getStatic(ValueOptionOps.getClass.asType, "MODULE$", ValueOptionOps.getClass.asType)
                .invokeV("min",
                  combinerVar.push().invokeV(combinerPropertyRef.getDeclaration.getName, combinerPropertyRef.getType.asType),
                  valueVar.push().invokeV(valuePropertyRef.getDeclaration.getName, valuePropertyRef.getType.asType))

            case PropertyFolding.Aggregation.COUNT =>
              getStatic(ValueOptionOps.getClass.asType, "MODULE$", ValueOptionOps.getClass.asType)
                .invokeV("inc",
                  combinerVar.push().invokeV(combinerPropertyRef.getDeclaration.getName, combinerPropertyRef.getType.asType))

            case _ => // NoOP
          }
        }
        `return`(combinerVar.push())
      }

      override def defMergeCombiners(mb: MethodBuilder, comb1Var: Var, comb2Var: Var): Unit = {
        import mb._
        propertyFoldings.foreach { folding =>
          val mapping = folding.getMapping
          val combinerPropertyRef = outputDataModelRef.findProperty(mapping.getDestinationProperty)
          folding.getAggregation match {
            case PropertyFolding.Aggregation.SUM =>
              getStatic(ValueOptionOps.getClass.asType, "MODULE$", ValueOptionOps.getClass.asType)
                .invokeV("add",
                  comb2Var.push().invokeV(combinerPropertyRef.getDeclaration.getName, combinerPropertyRef.getType.asType),
                  comb1Var.push().invokeV(combinerPropertyRef.getDeclaration.getName, combinerPropertyRef.getType.asType))

            case PropertyFolding.Aggregation.MAX =>
              getStatic(ValueOptionOps.getClass.asType, "MODULE$", ValueOptionOps.getClass.asType)
                .invokeV("max",
                  comb1Var.push().invokeV(combinerPropertyRef.getDeclaration.getName, combinerPropertyRef.getType.asType),
                  comb2Var.push().invokeV(combinerPropertyRef.getDeclaration.getName, combinerPropertyRef.getType.asType))

            case PropertyFolding.Aggregation.MIN =>
              getStatic(ValueOptionOps.getClass.asType, "MODULE$", ValueOptionOps.getClass.asType)
                .invokeV("min",
                  comb1Var.push().invokeV(combinerPropertyRef.getDeclaration.getName, combinerPropertyRef.getType.asType),
                  comb2Var.push().invokeV(combinerPropertyRef.getDeclaration.getName, combinerPropertyRef.getType.asType))

            case PropertyFolding.Aggregation.COUNT =>
              getStatic(ValueOptionOps.getClass.asType, "MODULE$", ValueOptionOps.getClass.asType)
                .invokeV("add",
                  comb2Var.push().invokeV(combinerPropertyRef.getDeclaration.getName, combinerPropertyRef.getType.asType),
                  comb1Var.push().invokeV(combinerPropertyRef.getDeclaration.getName, combinerPropertyRef.getType.asType))

            case _ => // NoOP
          }
        }
        `return`(comb1Var.push())
      }
    }

    context.jpContext.addClass(builder)
  }
}
