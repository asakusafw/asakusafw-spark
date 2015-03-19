package com.asakusafw.spark.compiler.subplan

import scala.collection.mutable
import scala.reflect.NameTransformer

import org.objectweb.asm.Type
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.lang.compiler.api.JobflowProcessor.{ Context => JPContext }
import com.asakusafw.lang.compiler.model.graph._
import com.asakusafw.lang.compiler.planning.SubPlan
import com.asakusafw.lang.compiler.planning.spark.NextDominantOperator
import com.asakusafw.spark.compiler.operator.aggregation.AggregationClassBuilder
import com.asakusafw.spark.compiler.spi.AggregationCompiler
import com.asakusafw.spark.runtime.fragment.Aggregation
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._

trait AggregationsField extends ClassBuilder {

  def flowId: String

  def jpContext: JPContext

  def subplanOutputs: Seq[SubPlan.Output]

  def defAggregationsField(fieldDef: FieldDef): Unit = {
    fieldDef.newFinalField("aggregations", classOf[Map[_, _]].asType,
      new TypeSignatureBuilder()
        .newClassType(classOf[Map[_, _]].asType) {
          _.newTypeArgument(SignatureVisitor.INSTANCEOF, Type.LONG_TYPE)
            .newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[Aggregation[_, _, _]].asType)
        }
        .build())
  }

  def initAggregationsField(mb: MethodBuilder): Unit = {
    import mb._
    thisVar.push().putField("aggregations", classOf[Map[_, _]].asType, initAggregations(mb))
  }

  def initAggregations(mb: MethodBuilder): Stack = {
    import mb._
    val builder = getStatic(Map.getClass.asType, "MODULE$", Map.getClass.asType)
      .invokeV("newBuilder", classOf[mutable.Builder[_, _]].asType)
    subplanOutputs.sortBy(_.getOperator.getOriginalSerialNumber).foreach { output =>
      val op = output.getOperator
      Option(output.getAttribute(classOf[NextDominantOperator])).foreach { dominant =>
        if (dominant.getNextDominantOperator.isInstanceOf[UserOperator]) {
          val operator = dominant.getNextDominantOperator.asInstanceOf[UserOperator]
          if (AggregationCompiler(jpContext.getClassLoader)
            .contains(operator.getAnnotation.resolve(jpContext.getClassLoader).annotationType)) {

            builder.invokeI(NameTransformer.encode("+="),
              classOf[mutable.Builder[_, _]].asType, {
                getStatic(Tuple2.getClass.asType, "MODULE$", Tuple2.getClass.asType)
                  .invokeV("apply", classOf[(_, _)].asType,
                    ldc(op.getOriginalSerialNumber).box().asType(classOf[AnyRef].asType),
                    pushNew0(AggregationClassBuilder.getOrCompile(flowId, operator, jpContext)).asType(classOf[AnyRef].asType))
              }.asType(classOf[AnyRef].asType))
          }
        }
      }
    }
    builder.invokeI("result", classOf[AnyRef].asType).cast(classOf[Map[_, _]].asType)
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
            _.newTypeArgument(SignatureVisitor.INSTANCEOF, Type.LONG_TYPE.boxed)
              .newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[Aggregation[_, _, _]].asType)
          }
        }
        .build()) { mb =>
        import mb._
        `return`(thisVar.push().getField("aggregations", classOf[Map[_, _]].asType))
      }
  }
}
