package com.asakusafw.spark.compiler
package operator.aggregation

import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable

import org.objectweb.asm._
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.lang.compiler.api.JobflowProcessor.{ Context => JPContext }
import com.asakusafw.lang.compiler.model.graph.UserOperator
import com.asakusafw.spark.compiler.spi.AggregationCompiler
import com.asakusafw.spark.runtime.fragment.Aggregation
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._

abstract class AggregationClassBuilder(
  val flowId: String,
  val keyType: Type,
  val valueType: Type,
  val combinerType: Type)
    extends ClassBuilder(
      Type.getType(s"L${GeneratedClassPackageInternalName}/${flowId}/fragment/Aggregation$$${AggregationClassBuilder.nextId};"),
      Option(AggregationClassBuilder.signature(keyType, valueType, combinerType)),
      classOf[Aggregation[_, _, _]].asType) {

  override def defMethods(methodDef: MethodDef): Unit = {
    super.defMethods(methodDef)

    methodDef.newMethod("mapSideCombine", Type.BOOLEAN_TYPE, Seq.empty)(defMapSideCombiner)

    methodDef.newMethod("createCombiner", classOf[AnyRef].asType, Seq(classOf[AnyRef].asType)) { mb =>
      import mb._
      val valueVar = `var`(classOf[AnyRef].asType, thisVar.nextLocal)
      `return`(
        thisVar.push().invokeV("createCombiner", combinerType, valueVar.push().cast(valueType)))
    }

    methodDef.newMethod("createCombiner", combinerType, Seq(valueType)) { mb =>
      import mb._
      val valueVar = `var`(valueType, thisVar.nextLocal)
      defCreateCombiner(mb, valueVar)
    }

    methodDef.newMethod("mergeValue", classOf[AnyRef].asType, Seq(classOf[AnyRef].asType, classOf[AnyRef].asType)) { mb =>
      import mb._
      val combinerVar = `var`(classOf[AnyRef].asType, thisVar.nextLocal)
      val valueVar = `var`(classOf[AnyRef].asType, combinerVar.nextLocal)
      `return`(
        thisVar.push().invokeV("mergeValue", combinerType,
          combinerVar.push().cast(combinerType), valueVar.push().cast(valueType)))
    }

    methodDef.newMethod("mergeValue", combinerType, Seq(combinerType, valueType)) { mb =>
      import mb._
      val combinerVar = `var`(combinerType, thisVar.nextLocal)
      val valueVar = `var`(valueType, combinerVar.nextLocal)
      defMergeValue(mb, combinerVar, valueVar)
    }

    methodDef.newMethod("mergeCombiners", classOf[AnyRef].asType, Seq(classOf[AnyRef].asType, classOf[AnyRef].asType)) { mb =>
      import mb._
      val comb1Var = `var`(classOf[AnyRef].asType, thisVar.nextLocal)
      val comb2Var = `var`(classOf[AnyRef].asType, comb1Var.nextLocal)
      `return`(
        thisVar.push().invokeV("mergeCombiners", combinerType,
          comb1Var.push().cast(combinerType), comb2Var.push().cast(combinerType)))
    }

    methodDef.newMethod("mergeCombiners", combinerType, Seq(combinerType, combinerType)) { mb =>
      import mb._
      val comb1Var = `var`(combinerType, thisVar.nextLocal)
      val comb2Var = `var`(combinerType, comb1Var.nextLocal)
      defMergeCombiners(mb, comb1Var, comb2Var)
    }
  }

  def defMapSideCombiner(mb: MethodBuilder): Unit
  def defCreateCombiner(mb: MethodBuilder, valueVar: Var): Unit
  def defMergeValue(mb: MethodBuilder, combinerVar: Var, valueVar: Var): Unit
  def defMergeCombiners(mb: MethodBuilder, comb1Var: Var, comb2Var: Var): Unit
}

object AggregationClassBuilder {

  private[this] val curId: AtomicLong = new AtomicLong(0L)

  def nextId: Long = curId.getAndIncrement

  def signature(keyType: Type, valueType: Type, combinerType: Type): String = {
    new ClassSignatureBuilder()
      .newSuperclass {
        _.newClassType(classOf[Aggregation[_, _, _]].asType) {
          _.newTypeArgument(SignatureVisitor.INSTANCEOF, keyType)
            .newTypeArgument(SignatureVisitor.INSTANCEOF, valueType)
            .newTypeArgument(SignatureVisitor.INSTANCEOF, combinerType)
        }
      }
      .build()
  }

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
