package com.asakusafw.spark.compiler.subplan

import scala.collection.mutable
import scala.reflect.NameTransformer

import org.objectweb.asm.{ Opcodes, Type }
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.lang.compiler.api.JobflowProcessor.{ Context => JPContext }
import com.asakusafw.lang.compiler.model.graph._
import com.asakusafw.lang.compiler.planning.SubPlan
import com.asakusafw.lang.compiler.planning.spark.NextDominantOperator
import com.asakusafw.spark.compiler.operator.aggregation.AggregationClassBuilder
import com.asakusafw.spark.compiler.spi.AggregationCompiler
import com.asakusafw.spark.runtime.aggregation.Aggregation
import com.asakusafw.spark.runtime.driver.ShuffleKey
import com.asakusafw.spark.runtime.rdd.BranchKey
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._

trait AggregationsField extends ClassBuilder {

  def flowId: String

  def jpContext: JPContext

  def branchKeys: BranchKeysClassBuilder

  def subplanOutputs: Seq[SubPlan.Output]

  def defAggregationsField(fieldDef: FieldDef): Unit = {
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

  def getAggregationsField(mb: MethodBuilder): Stack = {
    import mb._
    thisVar.push().invokeV("aggregations", classOf[Map[_, _]].asType)
  }

  def defAggregations(methodDef: MethodDef): Unit = {
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
        import mb._
        thisVar.push().getField("aggregations", classOf[Map[_, _]].asType).unlessNotNull {
          thisVar.push().putField("aggregations", classOf[Map[_, _]].asType, initAggregations(mb))
        }
        `return`(thisVar.push().getField("aggregations", classOf[Map[_, _]].asType))
      }
  }

  private def initAggregations(mb: MethodBuilder): Stack = {
    import mb._
    val builder = getStatic(Map.getClass.asType, "MODULE$", Map.getClass.asType)
      .invokeV("newBuilder", classOf[mutable.Builder[_, _]].asType)
    subplanOutputs.sortBy(_.getOperator.getOriginalSerialNumber).foreach { output =>
      val op = output.getOperator
      Option(output.getAttribute(classOf[NextDominantOperator])).foreach { dominant =>
        if (dominant.getNextDominantOperator.isInstanceOf[UserOperator]) {
          val operator = dominant.getNextDominantOperator.asInstanceOf[UserOperator]
          if (AggregationCompiler.support(operator)(AggregationCompiler.Context(flowId, jpContext))) {
            builder.invokeI(NameTransformer.encode("+="),
              classOf[mutable.Builder[_, _]].asType, {
                getStatic(Tuple2.getClass.asType, "MODULE$", Tuple2.getClass.asType)
                  .invokeV("apply", classOf[(_, _)].asType,
                    getStatic(
                      branchKeys.thisType,
                      branchKeys.getField(op.getOriginalSerialNumber),
                      classOf[BranchKey].asType)
                      .asType(classOf[AnyRef].asType),
                    pushNew0(AggregationClassBuilder.getOrCompile(flowId, operator, jpContext)).asType(classOf[AnyRef].asType))
              }.asType(classOf[AnyRef].asType))
          }
        }
      }
    }
    builder.invokeI("result", classOf[AnyRef].asType).cast(classOf[Map[_, _]].asType)
  }
}
