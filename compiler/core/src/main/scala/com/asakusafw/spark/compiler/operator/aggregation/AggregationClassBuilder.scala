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
package operator.aggregation

import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable

import org.objectweb.asm._
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.lang.compiler.api.JobflowProcessor.{ Context => JPContext }
import com.asakusafw.lang.compiler.model.graph.UserOperator
import com.asakusafw.spark.compiler.operator.aggregation.AggregationClassBuilder._
import com.asakusafw.spark.compiler.spi.AggregationCompiler
import com.asakusafw.spark.runtime.aggregation.Aggregation
import com.asakusafw.spark.runtime.driver.ShuffleKey
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._

abstract class AggregationClassBuilder(
  val flowId: String,
  val valueType: Type,
  val combinerType: Type)
  extends ClassBuilder(
    Type.getType(
      s"L${GeneratedClassPackageInternalName}/${flowId}/fragment/Aggregation$$${nextId};"),
    new ClassSignatureBuilder()
      .newSuperclass {
        _.newClassType(classOf[Aggregation[_, _, _]].asType) {
          _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[ShuffleKey].asType)
            .newTypeArgument(SignatureVisitor.INSTANCEOF, valueType)
            .newTypeArgument(SignatureVisitor.INSTANCEOF, combinerType)
        }
      }
      .build(),
    classOf[Aggregation[_, _, _]].asType) {

  override def defMethods(methodDef: MethodDef): Unit = {
    super.defMethods(methodDef)

    methodDef.newMethod("mapSideCombine", Type.BOOLEAN_TYPE, Seq.empty)(defMapSideCombine)

    methodDef.newMethod("newCombiner", classOf[AnyRef].asType, Seq.empty) { mb =>
      import mb._ // scalastyle:ignore
      `return`(thisVar.push().invokeV("newCombiner", combinerType))
    }

    methodDef.newMethod("newCombiner", combinerType, Seq.empty)(defNewCombiner)

    methodDef.newMethod(
      "initCombinerByValue",
      classOf[AnyRef].asType,
      Seq(classOf[AnyRef].asType, classOf[AnyRef].asType)) { mb =>
        import mb._ // scalastyle:ignore
        val combinerVar = `var`(classOf[AnyRef].asType, thisVar.nextLocal)
        val valueVar = `var`(classOf[AnyRef].asType, combinerVar.nextLocal)
        `return`(
          thisVar.push().invokeV("initCombinerByValue", combinerType,
            combinerVar.push().cast(combinerType), valueVar.push().cast(valueType)))
      }

    methodDef.newMethod(
      "initCombinerByValue",
      combinerType,
      Seq(combinerType, valueType)) { mb =>
        import mb._ // scalastyle:ignore
        val combinerVar = `var`(combinerType, thisVar.nextLocal)
        val valueVar = `var`(valueType, combinerVar.nextLocal)
        defInitCombinerByValue(mb, combinerVar, valueVar)
      }

    methodDef.newMethod(
      "mergeValue",
      classOf[AnyRef].asType,
      Seq(classOf[AnyRef].asType, classOf[AnyRef].asType)) { mb =>
        import mb._ // scalastyle:ignore
        val combinerVar = `var`(classOf[AnyRef].asType, thisVar.nextLocal)
        val valueVar = `var`(classOf[AnyRef].asType, combinerVar.nextLocal)
        `return`(
          thisVar.push().invokeV("mergeValue", combinerType,
            combinerVar.push().cast(combinerType), valueVar.push().cast(valueType)))
      }

    methodDef.newMethod(
      "mergeValue",
      combinerType,
      Seq(combinerType, valueType)) { mb =>
        import mb._ // scalastyle:ignore
        val combinerVar = `var`(combinerType, thisVar.nextLocal)
        val valueVar = `var`(valueType, combinerVar.nextLocal)
        defMergeValue(mb, combinerVar, valueVar)
      }

    methodDef.newMethod(
      "initCombinerByCombiner",
      classOf[AnyRef].asType,
      Seq(classOf[AnyRef].asType, classOf[AnyRef].asType)) { mb =>
        import mb._ // scalastyle:ignore
        val comb1Var = `var`(classOf[AnyRef].asType, thisVar.nextLocal)
        val comb2Var = `var`(classOf[AnyRef].asType, comb1Var.nextLocal)
        `return`(
          thisVar.push().invokeV("initCombinerByCombiner", combinerType,
            comb1Var.push().cast(combinerType), comb2Var.push().cast(combinerType)))
      }

    methodDef.newMethod(
      "initCombinerByCombiner",
      combinerType,
      Seq(combinerType, combinerType)) { mb =>
        import mb._ // scalastyle:ignore
        val comb1Var = `var`(combinerType, thisVar.nextLocal)
        val comb2Var = `var`(combinerType, comb1Var.nextLocal)
        defInitCombinerByCombiner(mb, comb1Var, comb2Var)
      }

    methodDef.newMethod(
      "mergeCombiners",
      classOf[AnyRef].asType,
      Seq(classOf[AnyRef].asType, classOf[AnyRef].asType)) { mb =>
        import mb._ // scalastyle:ignore
        val comb1Var = `var`(classOf[AnyRef].asType, thisVar.nextLocal)
        val comb2Var = `var`(classOf[AnyRef].asType, comb1Var.nextLocal)
        `return`(
          thisVar.push().invokeV("mergeCombiners", combinerType,
            comb1Var.push().cast(combinerType), comb2Var.push().cast(combinerType)))
      }

    methodDef.newMethod(
      "mergeCombiners",
      combinerType,
      Seq(combinerType, combinerType)) { mb =>
        import mb._ // scalastyle:ignore
        val comb1Var = `var`(combinerType, thisVar.nextLocal)
        val comb2Var = `var`(combinerType, comb1Var.nextLocal)
        defMergeCombiners(mb, comb1Var, comb2Var)
      }
  }

  def defMapSideCombine(mb: MethodBuilder): Unit
  def defNewCombiner(mb: MethodBuilder): Unit
  def defInitCombinerByValue(mb: MethodBuilder, combinerVar: Var, valueVar: Var): Unit
  def defMergeValue(mb: MethodBuilder, combinerVar: Var, valueVar: Var): Unit
  def defInitCombinerByCombiner(mb: MethodBuilder, comb1Var: Var, comb2Var: Var): Unit
  def defMergeCombiners(mb: MethodBuilder, comb1Var: Var, comb2Var: Var): Unit
}

object AggregationClassBuilder {

  private[this] val curId: AtomicLong = new AtomicLong(0L)

  def nextId: Long = curId.getAndIncrement

  private[this] val cache: mutable.Map[JPContext, mutable.Map[(String, Long), Type]] =
    mutable.WeakHashMap.empty

  def getOrCompile(
    flowId: String,
    operator: UserOperator,
    jpContext: JPContext): Type = {
    cache.getOrElseUpdate(jpContext, mutable.Map.empty).getOrElseUpdate(
      (flowId, operator.getOriginalSerialNumber), {
        AggregationCompiler.compile(operator)(AggregationCompiler.Context(flowId, jpContext))
      })
  }
}
