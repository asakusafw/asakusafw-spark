package com.asakusafw.spark.compiler
package operator
package aggregation

import scala.collection.JavaConversions._
import scala.reflect.ClassTag

import org.objectweb.asm.Type

import com.asakusafw.lang.compiler.model.graph._
import com.asakusafw.spark.compiler.spi.AggregationCompiler
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._
import com.asakusafw.vocabulary.flow.processor.PartialAggregation
import com.asakusafw.vocabulary.operator.Fold

class FoldAggregationCompiler extends AggregationCompiler {

  def of: Class[_] = classOf[Fold]

  def compile(operator: UserOperator)(implicit context: Context): Type = {

    val operatorInfo = new OperatorInfo(operator)(context.jpContext)
    import operatorInfo._

    assert(annotationDesc.resolveClass == of)
    assert(inputs.size == 1)
    assert(outputs.size == 1)
    assert(inputs(Fold.ID_INPUT).dataModelType == outputs(Fold.ID_OUTPUT).dataModelType)

    methodDesc.parameterClasses
      .zip(inputs(Fold.ID_INPUT).dataModelClass
        +: outputs(Fold.ID_OUTPUT).dataModelClass
        +: arguments.map(_.resolveClass))
      .foreach {
        case (method, model) => assert(method.isAssignableFrom(model))
      }

    val builder = new AggregationClassBuilder(
      context.flowId,
      classOf[Seq[_]].asType,
      inputs(Fold.ID_INPUT).dataModelType,
      outputs(Fold.ID_OUTPUT).dataModelType) with OperatorField {

      override val operatorType: Type = implementationClassType

      override def defFields(fieldDef: FieldDef): Unit = {
        defOperatorField(fieldDef)
      }

      override def defMethods(methodDef: MethodDef): Unit = {
        super.defMethods(methodDef)
        defGetOperator(methodDef)
      }

      override def defMapSideCombiner(mb: MethodBuilder): Unit = {
        import mb._
        val partialAggregation = annotationDesc.getElements()("partialAggregation")
          .resolve(context.jpContext.getClassLoader).asInstanceOf[PartialAggregation]
        `return`(ldc(partialAggregation == PartialAggregation.PARTIAL))
      }

      override def defCreateCombiner(mb: MethodBuilder, valueVar: Var): Unit = {
        import mb._
        `return`(valueVar.push())
      }

      override def defMergeValue(mb: MethodBuilder, combinerVar: Var, valueVar: Var): Unit = {
        import mb._
        getOperatorField(mb).invokeV(
          methodDesc.getName,
          combinerVar.push().asType(methodDesc.asType.getArgumentTypes()(0))
            +: valueVar.push().asType(methodDesc.asType.getArgumentTypes()(1))
            +: arguments.map { argument =>
              ldc(argument.value)(ClassTag(argument.resolveClass))
            }: _*)
        `return`(combinerVar.push())
      }

      override def defMergeCombiners(mb: MethodBuilder, comb1Var: Var, comb2Var: Var): Unit = {
        import mb._
        `return`(thisVar.push().invokeV("mergeValue", combinerType, comb1Var.push(), comb2Var.push()))
      }
    }

    context.jpContext.addClass(builder)
  }
}
