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

    val operatorInfo = new OperatorInfo(operator)(context.jpContext)
    import operatorInfo._

    assert(annotationDesc.resolveClass == of,
      s"The operator type is not supported: ${annotationDesc.resolveClass.getSimpleName}")
    assert(inputs.size == 1,
      s"The size of inputs should be 1: ${inputs.size}")
    assert(outputs.size == 1,
      s"The size of outputs should be 1: ${outputs.size}")

    val propertyFoldings = SummarizedModelUtil.getPropertyFoldings(context.jpContext.getClassLoader, operator).toSeq

    val builder = new AggregationClassBuilder(
      context.flowId,
      inputs(Summarize.ID_INPUT).dataModelType,
      outputs(Summarize.ID_OUTPUT).dataModelType) {

      override def defMapSideCombine(mb: MethodBuilder): Unit = {
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
          val valuePropertyRef = inputs(Summarize.ID_INPUT).dataModelRef.findProperty(mapping.getSourceProperty)
          val combinerPropertyRef = outputs(Summarize.ID_OUTPUT).dataModelRef.findProperty(mapping.getDestinationProperty)
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
        `return`(thisVar.push().invokeV("mergeValue", combinerType, combinerVar.push(), valueVar.push()))
      }

      override def defMergeValue(mb: MethodBuilder, combinerVar: Var, valueVar: Var): Unit = {
        import mb._
        propertyFoldings.foreach { folding =>
          val mapping = folding.getMapping
          val valuePropertyRef = inputs(Summarize.ID_INPUT).dataModelRef.findProperty(mapping.getSourceProperty)
          val combinerPropertyRef = outputs(Summarize.ID_OUTPUT).dataModelRef.findProperty(mapping.getDestinationProperty)
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
          val combinerPropertyRef = outputs(Summarize.ID_OUTPUT).dataModelRef.findProperty(mapping.getDestinationProperty)
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
