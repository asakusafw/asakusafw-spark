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
package subplan

import scala.collection.mutable
import scala.reflect.NameTransformer

import org.objectweb.asm.{ Opcodes, Type }
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.lang.compiler.model.graph._
import com.asakusafw.lang.compiler.planning.SubPlan
import com.asakusafw.spark.compiler.operator.aggregation.AggregationClassBuilder
import com.asakusafw.spark.compiler.planning.SubPlanOutputInfo
import com.asakusafw.spark.compiler.spi.AggregationCompiler
import com.asakusafw.spark.runtime.aggregation.Aggregation
import com.asakusafw.spark.runtime.driver.ShuffleKey
import com.asakusafw.spark.runtime.rdd.BranchKey
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._

trait AggregationsField extends ClassBuilder {

  implicit def context: SparkClientCompiler.Context

  def subplanOutputs: Seq[SubPlan.Output]

  override def defFields(fieldDef: FieldDef): Unit = {
    super.defFields(fieldDef)

    fieldDef.newField(
      Opcodes.ACC_PRIVATE | Opcodes.ACC_TRANSIENT,
      "aggregations", classOf[Map[_, _]].asType,
      new TypeSignatureBuilder()
        .newClassType(classOf[Map[_, _]].asType) {
          _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[BranchKey].asType)
            .newTypeArgument(SignatureVisitor.INSTANCEOF) {
              _.newClassType(classOf[Aggregation[_, _, _]].asType) {
                _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[ShuffleKey].asType)
                  .newTypeArgument()
                  .newTypeArgument()
              }
            }
        }
        .build())
  }

  override def defMethods(methodDef: MethodDef): Unit = {
    super.defMethods(methodDef)

    methodDef.newMethod("aggregations", classOf[Map[_, _]].asType, Seq.empty,
      new MethodSignatureBuilder()
        .newReturnType {
          _.newClassType(classOf[Map[_, _]].asType) {
            _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[BranchKey].asType)
              .newTypeArgument(SignatureVisitor.INSTANCEOF) {
                _.newClassType(classOf[Aggregation[_, _, _]].asType) {
                  _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[ShuffleKey].asType)
                    .newTypeArgument()
                    .newTypeArgument()
                }
              }
          }
        }
        .build()) { mb =>
        import mb._ // scalastyle:ignore
        thisVar.push().getField("aggregations", classOf[Map[_, _]].asType).unlessNotNull {
          thisVar.push().putField("aggregations", classOf[Map[_, _]].asType, initAggregations(mb))
        }
        `return`(thisVar.push().getField("aggregations", classOf[Map[_, _]].asType))
      }
  }

  def getAggregationsField(mb: MethodBuilder): Stack = {
    import mb._ // scalastyle:ignore
    thisVar.push().invokeV("aggregations", classOf[Map[_, _]].asType)
  }

  private def initAggregations(mb: MethodBuilder): Stack = {
    import mb._ // scalastyle:ignore
    val builder = getStatic(Map.getClass.asType, "MODULE$", Map.getClass.asType)
      .invokeV("newBuilder", classOf[mutable.Builder[_, _]].asType)
    for {
      output <- subplanOutputs.sortBy(_.getOperator.getSerialNumber)
      outputInfo <- Option(output.getAttribute(classOf[SubPlanOutputInfo]))
      if outputInfo.getAggregationInfo.isInstanceOf[UserOperator]
      operator = outputInfo.getAggregationInfo.asInstanceOf[UserOperator]
      if (AggregationCompiler.support(operator))
    } {
      builder.invokeI(NameTransformer.encode("+="),
        classOf[mutable.Builder[_, _]].asType, {
          getStatic(Tuple2.getClass.asType, "MODULE$", Tuple2.getClass.asType)
            .invokeV("apply", classOf[(_, _)].asType,
              context.branchKeys.getField(mb, output.getOperator)
                .asType(classOf[AnyRef].asType),
              pushNew0(AggregationClassBuilder.getOrCompile(operator))
                .asType(classOf[AnyRef].asType))
        }.asType(classOf[AnyRef].asType))
    }
    builder.invokeI("result", classOf[AnyRef].asType).cast(classOf[Map[_, _]].asType)
  }
}
