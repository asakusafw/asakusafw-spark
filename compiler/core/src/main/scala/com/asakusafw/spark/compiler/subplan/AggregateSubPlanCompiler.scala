package com.asakusafw.spark.compiler
package subplan

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.reflect.{ ClassTag, NameTransformer }

import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.objectweb.asm.Type
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.lang.compiler.model.graph._
import com.asakusafw.lang.compiler.planning.{ PlanMarker, SubPlan }
import com.asakusafw.lang.compiler.planning.spark.{ DominantOperator, PartitioningParameters }
import com.asakusafw.spark.compiler.operator.{ EdgeFragmentClassBuilder, OperatorInfo, OutputFragmentClassBuilder }
import com.asakusafw.spark.compiler.operator.aggregation.AggregationClassBuilder
import com.asakusafw.spark.compiler.spi.{ AggregationCompiler, OperatorCompiler, OperatorType, SubPlanCompiler }
import com.asakusafw.spark.runtime.aggregation.Aggregation
import com.asakusafw.spark.runtime.driver.ShuffleKey
import com.asakusafw.spark.runtime.fragment.{ Fragment, OutputFragment }
import com.asakusafw.spark.runtime.rdd.BranchKey
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._
import com.asakusafw.vocabulary.flow.processor.PartialAggregation
import com.asakusafw.vocabulary.operator.Fold

class AggregateSubPlanCompiler extends SubPlanCompiler {

  import AggregateSubPlanCompiler._

  override def support(operator: Operator)(implicit context: Context): Boolean = {
    AggregationCompiler.support(operator)(AggregationCompiler.Context(context.flowId, context.jpContext))
  }

  override def instantiator: Instantiator = AggregateDriverInstantiator

  override def compile(subplan: SubPlan)(implicit context: Context): Type = {
    val dominant = subplan.getAttribute(classOf[DominantOperator]).getDominantOperator
    assert(dominant.isInstanceOf[UserOperator],
      s"The dominant operator should be user operator: ${dominant}")
    val operator = dominant.asInstanceOf[UserOperator]

    val operatorInfo = new OperatorInfo(operator)(context.jpContext)
    import operatorInfo._

    assert(inputs.size == 1,
      s"The size of inputs should be 1: ${inputs.size}")
    assert(outputs.size == 1,
      s"The size of outputs should be 1: ${outputs.size}")

    val builder = new AggregateDriverClassBuilder(
      context.flowId,
      inputs.head.dataModelType,
      outputs.head.dataModelType) {

      override val jpContext = context.jpContext

      override val branchKeys: BranchKeysClassBuilder = context.branchKeys

      override val dominantOperator = operator

      override val subplanOutputs: Seq[SubPlan.Output] = subplan.getOutputs.toSeq

      override def defMethods(methodDef: MethodDef): Unit = {
        super.defMethods(methodDef)

        methodDef.newMethod("fragments", classOf[(_, _)].asType, Seq.empty,
          new MethodSignatureBuilder()
            .newReturnType {
              _.newClassType(classOf[(_, _)].asType) {
                _.newTypeArgument(SignatureVisitor.INSTANCEOF) {
                  _.newClassType(classOf[Fragment[_]].asType) {
                    _.newTypeArgument(SignatureVisitor.INSTANCEOF, combinerType)
                  }
                }
                  .newTypeArgument(SignatureVisitor.INSTANCEOF) {
                    _.newClassType(classOf[Map[_, _]].asType) {
                      _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[BranchKey].asType)
                        .newTypeArgument(SignatureVisitor.INSTANCEOF) {
                          _.newClassType(classOf[OutputFragment[_]].asType) {
                            _.newTypeArgument()
                          }
                        }
                    }
                  }
              }
            }
            .build()) { mb =>
            import mb._
            val nextLocal = new AtomicInteger(thisVar.nextLocal)

            val fragmentBuilder = new FragmentTreeBuilder(mb, nextLocal)(
              OperatorCompiler.Context(
                flowId = context.flowId,
                jpContext = context.jpContext,
                branchKeys = context.branchKeys,
                broadcastIds = context.broadcastIds,
                shuffleKeyTypes = context.shuffleKeyTypes))
            val fragmentVar = fragmentBuilder.build(operator.getOutputs.head)
            val outputsVar = fragmentBuilder.buildOutputsVar(subplanOutputs)

            `return`(
              getStatic(Tuple2.getClass.asType, "MODULE$", Tuple2.getClass.asType).
                invokeV("apply", classOf[(_, _)].asType,
                  fragmentVar.push().asType(classOf[AnyRef].asType), outputsVar.push().asType(classOf[AnyRef].asType)))
          }

        methodDef.newMethod("aggregation", classOf[Aggregation[_, _, _]].asType, Seq.empty,
          new MethodSignatureBuilder()
            .newReturnType {
              _.newClassType(classOf[Aggregation[_, _, _]].asType) {
                _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[ShuffleKey].asType)
                  .newTypeArgument(SignatureVisitor.INSTANCEOF, valueType)
                  .newTypeArgument(SignatureVisitor.INSTANCEOF, combinerType)
              }
            }
            .build()) { mb =>
            import mb._
            val aggregationType = AggregationClassBuilder.getOrCompile(context.flowId, operator, context.jpContext)
            `return`(pushNew0(aggregationType))
          }
      }
    }

    context.shuffleKeyTypes ++= builder.shuffleKeyTypes.map(_._2._1)
    context.jpContext.addClass(builder)
  }
}

object AggregateSubPlanCompiler {

  object AggregateDriverInstantiator extends Instantiator {

    override def newInstance(
      driverType: Type,
      subplan: SubPlan)(implicit context: Context): Var = {
      import context.mb._

      val dominant = subplan.getAttribute(classOf[DominantOperator]).getDominantOperator.asInstanceOf[UserOperator]

      val operatorInfo = new OperatorInfo(dominant)(context.jpContext)
      import operatorInfo._

      assert(inputs.size == 1,
        s"The size of inputs should be 1: ${inputs.size}")
      val input = inputs.head
      val dataModelRef = input.dataModelRef
      val properties = input.getGroup.getGrouping.map { grouping =>
        dataModelRef.findProperty(grouping).getType.asType
      }.toSeq

      val partitioner = pushNew(classOf[HashPartitioner].asType)
      partitioner.dup().invokeInit(
        context.scVar.push()
          .invokeV("defaultParallelism", Type.INT_TYPE))
      val partitionerVar = partitioner.store(context.nextLocal.getAndAdd(partitioner.size))

      val aggregateDriver = pushNew(driverType)
      aggregateDriver.dup().invokeInit(
        context.scVar.push(),
        context.hadoopConfVar.push(),
        context.broadcastsVar.push(), {
          val builder = getStatic(Seq.getClass.asType, "MODULE$", Seq.getClass.asType)
            .invokeV("newBuilder", classOf[mutable.Builder[_, _]].asType)

          subplan.getInputs.toSet[SubPlan.Input]
            .filter { input =>
              val marker = input.getOperator.getAttribute(classOf[PlanMarker])
              marker == PlanMarker.CHECKPOINT || marker == PlanMarker.GATHER
            }
            .flatMap(input => input.getOpposites.toSet[SubPlan.Output])
            .map(_.getOperator.getSerialNumber)
            .foreach { sn =>
              builder.invokeI(NameTransformer.encode("+="), classOf[mutable.Builder[_, _]].asType,
                context.rddsVar.push().invokeI(
                  "apply",
                  classOf[AnyRef].asType,
                  getStatic(
                    context.branchKeys.thisType,
                    context.branchKeys.getField(sn),
                    classOf[BranchKey].asType).asType(classOf[AnyRef].asType)))
            }

          builder.invokeI("result", classOf[AnyRef].asType).cast(classOf[Seq[_]].asType)
        }, {
          val builder = getStatic(Seq.getClass.asType, "MODULE$", Seq.getClass.asType)
            .invokeV("newBuilder", classOf[mutable.Builder[_, _]].asType)

          input.getGroup.getOrdering.foreach { ordering =>
            builder.invokeI(
              NameTransformer.encode("+="),
              classOf[mutable.Builder[_, _]].asType,
              ldc(ordering.getDirection == Group.Direction.ASCENDANT).box().asType(classOf[AnyRef].asType))
          }

          builder.invokeI("result", classOf[AnyRef].asType).cast(classOf[Seq[_]].asType)
        },
        partitionerVar.push().asType(classOf[Partitioner].asType))
      aggregateDriver.store(context.nextLocal.getAndAdd(aggregateDriver.size))
    }
  }
}
