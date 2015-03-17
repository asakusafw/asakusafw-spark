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
    val annotationDesc = operator.getAnnotation
    assert(annotationDesc.getDeclaringClass.resolve(context.jpContext.getClassLoader) == of)
    val methodDesc = operator.getMethod
    val methodType = Type.getType(methodDesc.resolve(context.jpContext.getClassLoader))
    val implementationClassType = operator.getImplementationClass.asType

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

    assert(inputDataModelType == outputDataModelType)

    val arguments = operator.getArguments.toSeq

    assert(methodType.getArgumentTypes.toSeq ==
      Seq(inputDataModelType, outputDataModelType)
      ++ arguments.map(_.getValue.getValueType.asType))

    val builder = new AggregationClassBuilder(
      context.flowId, classOf[Seq[_]].asType, inputDataModelType, outputDataModelType) with OperatorField {

      override val operatorType: Type = implementationClassType

      override def defFields(fieldDef: FieldDef): Unit = {
        defOperatorField(fieldDef)
      }

      override def defConstructors(ctorDef: ConstructorDef): Unit = {
        ctorDef.newInit(Seq.empty) { mb =>
          import mb._
          thisVar.push().invokeInit(superType)
          initOperatorField(mb)
        }
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
        getOperatorField(mb).invokeV(methodDesc.getName,
          Seq(combinerVar.push(), valueVar.push())
            ++ arguments.map { argument =>
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
