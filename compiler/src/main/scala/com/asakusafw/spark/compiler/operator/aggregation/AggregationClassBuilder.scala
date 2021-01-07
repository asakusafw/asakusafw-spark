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
package operator.aggregation

import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable

import org.objectweb.asm._
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.lang.compiler.model.graph.UserOperator
import com.asakusafw.spark.compiler.operator.aggregation.AggregationClassBuilder._
import com.asakusafw.spark.compiler.spi.AggregationCompiler
import com.asakusafw.spark.runtime.aggregation.Aggregation
import com.asakusafw.spark.runtime.rdd.ShuffleKey
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._

abstract class AggregationClassBuilder(
  val valueType: Type,
  val combinerType: Type)(
    val aggregationType: AggregationType)(
      implicit val context: AggregationCompiler.Context)
  extends ClassBuilder(
    Type.getType(
      s"L${GeneratedClassPackageInternalName}/${context.flowId}/fragment/${nextName(aggregationType)};"), // scalastyle:ignore
    new ClassSignatureBuilder()
      .newSuperclass {
        _.newClassType(classOf[Aggregation[_, _, _]].asType) {
          _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[ShuffleKey].asType)
            .newTypeArgument(SignatureVisitor.INSTANCEOF, valueType)
            .newTypeArgument(SignatureVisitor.INSTANCEOF, combinerType)
        }
      },
    classOf[Aggregation[_, _, _]].asType) {

  override def defMethods(methodDef: MethodDef): Unit = {
    super.defMethods(methodDef)

    methodDef.newMethod("newCombiner", classOf[AnyRef].asType, Seq.empty) { implicit mb =>
      val thisVar :: _ = mb.argVars
      `return`(thisVar.push().invokeV("newCombiner", combinerType))
    }

    methodDef.newMethod("newCombiner", combinerType, Seq.empty)(defNewCombiner()(_))

    methodDef.newMethod(
      "initCombinerByValue",
      classOf[AnyRef].asType,
      Seq(classOf[AnyRef].asType, classOf[AnyRef].asType)) { implicit mb =>
        val thisVar :: combinerVar :: valueVar :: _ = mb.argVars
        `return`(
          thisVar.push().invokeV("initCombinerByValue", combinerType,
            combinerVar.push().cast(combinerType), valueVar.push().cast(valueType)))
      }

    methodDef.newMethod(
      "initCombinerByValue",
      combinerType,
      Seq(combinerType, valueType)) { implicit mb =>
        val thisVar :: combinerVar :: valueVar :: _ = mb.argVars
        defInitCombinerByValue(combinerVar, valueVar)
      }

    methodDef.newMethod(
      "mergeValue",
      classOf[AnyRef].asType,
      Seq(classOf[AnyRef].asType, classOf[AnyRef].asType)) { implicit mb =>
        val thisVar :: combinerVar :: valueVar :: _ = mb.argVars
        `return`(
          thisVar.push().invokeV("mergeValue", combinerType,
            combinerVar.push().cast(combinerType), valueVar.push().cast(valueType)))
      }

    methodDef.newMethod(
      "mergeValue",
      combinerType,
      Seq(combinerType, valueType)) { implicit mb =>
        val thisVar :: combinerVar :: valueVar :: _ = mb.argVars
        defMergeValue(combinerVar, valueVar)
      }

    methodDef.newMethod(
      "initCombinerByCombiner",
      classOf[AnyRef].asType,
      Seq(classOf[AnyRef].asType, classOf[AnyRef].asType)) { implicit mb =>
        val thisVar :: comb1Var :: comb2Var :: _ = mb.argVars
        `return`(
          thisVar.push().invokeV("initCombinerByCombiner", combinerType,
            comb1Var.push().cast(combinerType), comb2Var.push().cast(combinerType)))
      }

    methodDef.newMethod(
      "initCombinerByCombiner",
      combinerType,
      Seq(combinerType, combinerType)) { implicit mb =>
        val thisVar :: comb1Var :: comb2Var :: _ = mb.argVars
        defInitCombinerByCombiner(comb1Var, comb2Var)
      }

    methodDef.newMethod(
      "mergeCombiners",
      classOf[AnyRef].asType,
      Seq(classOf[AnyRef].asType, classOf[AnyRef].asType)) { implicit mb =>
        val thisVar :: comb1Var :: comb2Var :: _ = mb.argVars
        `return`(
          thisVar.push().invokeV("mergeCombiners", combinerType,
            comb1Var.push().cast(combinerType), comb2Var.push().cast(combinerType)))
      }

    methodDef.newMethod(
      "mergeCombiners",
      combinerType,
      Seq(combinerType, combinerType)) { implicit mb =>
        val thisVar :: comb1Var :: comb2Var :: _ = mb.argVars
        defMergeCombiners(comb1Var, comb2Var)
      }
  }

  def defNewCombiner()(implicit mb: MethodBuilder): Unit
  def defInitCombinerByValue(combinerVar: Var, valueVar: Var)(implicit mb: MethodBuilder): Unit
  def defMergeValue(combinerVar: Var, valueVar: Var)(implicit mb: MethodBuilder): Unit
  def defInitCombinerByCombiner(comb1Var: Var, comb2Var: Var)(implicit mb: MethodBuilder): Unit
  def defMergeCombiners(comb1Var: Var, comb2Var: Var)(implicit mb: MethodBuilder): Unit
}

object AggregationClassBuilder {

  trait AggregationType

  object AggregationType {
    case object Fold extends AggregationType
    case object Summarize extends AggregationType
  }

  private[this] val curIds: mutable.Map[AggregationCompiler.Context, mutable.Map[AggregationType, AtomicLong]] = // scalastyle:ignore
    mutable.WeakHashMap.empty

  def nextName(
    aggregationType: AggregationType)(
      implicit context: AggregationCompiler.Context): String = {
    s"${aggregationType}Aggregation$$${
      curIds.getOrElseUpdate(context, mutable.Map.empty)
        .getOrElseUpdate(aggregationType, new AtomicLong(0))
        .getAndIncrement()
    }"
  }

  private[this] val cache: mutable.Map[AggregationCompiler.Context, mutable.Map[Long, Type]] = // scalastyle:ignore
    mutable.WeakHashMap.empty

  def getOrCompile(
    operator: UserOperator)(
      implicit context: AggregationCompiler.Context): Type = {
    cache.getOrElseUpdate(context, mutable.Map.empty)
      .getOrElseUpdate(
        operator.getOriginalSerialNumber,
        AggregationCompiler.compile(operator))
  }
}
