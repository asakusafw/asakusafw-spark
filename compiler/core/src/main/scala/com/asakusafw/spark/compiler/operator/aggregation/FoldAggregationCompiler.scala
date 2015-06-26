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
    import operatorInfo._ // scalastyle:ignore

    assert(annotationDesc.resolveClass == of,
      s"The operator type is not supported: ${annotationDesc.resolveClass.getSimpleName}")
    assert(inputs.size == 1,
      s"The size of inputs should be 1: ${inputs.size}")
    assert(outputs.size == 1,
      s"The size of outputs should be 1: ${outputs.size}")
    assert(inputs(Fold.ID_INPUT).dataModelType == outputs(Fold.ID_OUTPUT).dataModelType,
      s"The data models are not the same type: (${
        inputs(Fold.ID_INPUT).dataModelType
      }, ${
        outputs(Fold.ID_OUTPUT).dataModelType
      })")

    assert(
      methodDesc.parameterClasses
        .zip(inputs.map(_.dataModelClass)
          ++: outputs.map(_.dataModelClass)
          ++: arguments.map(_.resolveClass))
        .forall {
          case (method, model) => method.isAssignableFrom(model)
        },
      s"The operator method parameter types are not compatible: (${
        methodDesc.parameterClasses.map(_.getName).mkString("(", ",", ")")
      }, ${
        (inputs.map(_.dataModelClass)
          ++: outputs.map(_.dataModelClass)
          ++: arguments.map(_.resolveClass)).map(_.getName).mkString("(", ",", ")")
      })")

    val builder = new AggregationClassBuilder(
      context.flowId,
      inputs(Fold.ID_INPUT).dataModelType,
      outputs(Fold.ID_OUTPUT).dataModelType) with OperatorField {

      override val operatorType: Type = implementationClassType

      override def defConstructors(ctorDef: ConstructorDef): Unit = {
        ctorDef.newInit(Seq.empty) { mb =>
          import mb._ // scalastyle:ignore
          thisVar.push().invokeInit(superType)
          initOperatorField(mb)
        }
      }

      override def defMapSideCombine(mb: MethodBuilder): Unit = {
        import mb._ // scalastyle:ignore
        val partialAggregation = annotationDesc.getElements()("partialAggregation")
          .resolve(context.jpContext.getClassLoader).asInstanceOf[PartialAggregation]
        `return`(ldc(partialAggregation == PartialAggregation.PARTIAL))
      }

      override def defNewCombiner(mb: MethodBuilder): Unit = {
        import mb._ // scalastyle:ignore
        `return`(pushNew0(combinerType))
      }

      override def defInitCombinerByValue(mb: MethodBuilder, combinerVar: Var, valueVar: Var): Unit = {
        import mb._ // scalastyle:ignore
        combinerVar.push().invokeV("copyFrom", valueVar.push())
        `return`(combinerVar.push())
      }

      override def defMergeValue(mb: MethodBuilder, combinerVar: Var, valueVar: Var): Unit = {
        import mb._ // scalastyle:ignore
        getOperatorField(mb).invokeV(
          methodDesc.getName,
          combinerVar.push().asType(methodDesc.asType.getArgumentTypes()(0))
            +: valueVar.push().asType(methodDesc.asType.getArgumentTypes()(1))
            +: arguments.map { argument =>
              ldc(argument.value)(ClassTag(argument.resolveClass))
            }: _*)
        `return`(combinerVar.push())
      }

      override def defInitCombinerByCombiner(mb: MethodBuilder, comb1Var: Var, comb2Var: Var): Unit = {
        import mb._ // scalastyle:ignore
        comb1Var.push().invokeV("copyFrom", comb2Var.push())
        `return`(comb1Var.push())
      }

      override def defMergeCombiners(mb: MethodBuilder, comb1Var: Var, comb2Var: Var): Unit = {
        import mb._ // scalastyle:ignore
        `return`(thisVar.push().invokeV("mergeValue", combinerType, comb1Var.push(), comb2Var.push()))
      }
    }

    context.jpContext.addClass(builder)
  }
}
