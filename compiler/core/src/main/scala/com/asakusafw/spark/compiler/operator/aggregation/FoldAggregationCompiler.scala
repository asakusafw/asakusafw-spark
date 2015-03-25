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

    assert(operatorInfo.annotationClass == of)
    assert(operatorInfo.inputs.size == 1)
    assert(operatorInfo.outputs.size == 1)
    assert(operatorInfo.inputDataModelTypes(Fold.ID_INPUT) == operatorInfo.outputDataModelTypes(Fold.ID_OUTPUT))

    assert(operatorInfo.methodType.getArgumentTypes.toSeq ==
      Seq(operatorInfo.inputDataModelTypes(Fold.ID_INPUT),
        operatorInfo.outputDataModelTypes(Fold.ID_OUTPUT))
        ++ operatorInfo.argumentTypes)

    val builder = new AggregationClassBuilder(
      context.flowId,
      classOf[Seq[_]].asType,
      operatorInfo.inputDataModelTypes(Fold.ID_INPUT),
      operatorInfo.outputDataModelTypes(Fold.ID_OUTPUT)) with OperatorField {

      override val operatorType: Type = operatorInfo.implementationClassType

      override def defFields(fieldDef: FieldDef): Unit = {
        defOperatorField(fieldDef)
      }

      override def defMethods(methodDef: MethodDef): Unit = {
        super.defMethods(methodDef)
        defGetOperator(methodDef)
      }

      override def defMapSideCombiner(mb: MethodBuilder): Unit = {
        import mb._
        val partialAggregation = operatorInfo.annotationDesc.getElements()("partialAggregation")
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
          operatorInfo.methodDesc.getName,
          Seq(combinerVar.push(), valueVar.push())
            ++ operatorInfo.arguments.map { argument =>
              ldc(argument.getValue.resolve(context.jpContext.getClassLoader))(
                ClassTag(argument.getValue.getValueType.resolve(context.jpContext.getClassLoader)))
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
