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
import com.asakusafw.spark.compiler.operator.{ EdgeFragmentClassBuilder, OperatorInfo, OutputFragmentClassBuilder }
import com.asakusafw.spark.compiler.operator.aggregation.AggregationClassBuilder
import com.asakusafw.spark.compiler.planning.SubPlanInfo
import com.asakusafw.spark.compiler.spi.{ AggregationCompiler, OperatorCompiler, OperatorType, SubPlanCompiler }
import com.asakusafw.spark.runtime.aggregation.Aggregation
import com.asakusafw.spark.runtime.driver.{ BroadcastId, ShuffleKey }
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
    val primaryOperator = subplan.getAttribute(classOf[SubPlanInfo]).getPrimaryOperator
    assert(primaryOperator.isInstanceOf[UserOperator],
      s"The primary operator should be user operator: ${primaryOperator}")
    val operator = primaryOperator.asInstanceOf[UserOperator]

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

      override val branchKeys: BranchKeys = context.branchKeys

      override val dominantOperator = operator

      override val subplanOutputs: Seq[SubPlan.Output] = subplan.getOutputs.toSeq

      override def defMethods(methodDef: MethodDef): Unit = {
        super.defMethods(methodDef)

        methodDef.newMethod("fragments", classOf[(_, _)].asType, Seq(classOf[Map[BroadcastId, Broadcast[_]]].asType),
          new MethodSignatureBuilder()
            .newParameterType {
              _.newClassType(classOf[Map[_, _]].asType) {
                _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[BroadcastId].asType)
                  .newTypeArgument(SignatureVisitor.INSTANCEOF) {
                    _.newClassType(classOf[Broadcast[_]].asType) {
                      _.newTypeArgument()
                    }
                  }
              }
            }
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
            val broadcastsVar = `var`(classOf[Map[BroadcastId, Broadcast[_]]].asType, thisVar.nextLocal)
            val nextLocal = new AtomicInteger(broadcastsVar.nextLocal)

            val fragmentBuilder = new FragmentTreeBuilder(mb, broadcastsVar, nextLocal)(
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

      val primaryOperator = subplan.getAttribute(classOf[SubPlanInfo]).getPrimaryOperator.asInstanceOf[UserOperator]

      val operatorInfo = new OperatorInfo(primaryOperator)(context.jpContext)
      import operatorInfo._

      assert(inputs.size == 1,
        s"The size of inputs should be 1: ${inputs.size}")
      val input = inputs.head

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

          for {
            subPlanInput <- subplan.getInputs
            planMarker = subPlanInput.getOperator.getAttribute(classOf[PlanMarker])
            if planMarker == PlanMarker.CHECKPOINT || planMarker == PlanMarker.GATHER
            prevSubPlanOutput <- subPlanInput.getOpposites
            marker = prevSubPlanOutput.getOperator
          } {
            builder.invokeI(NameTransformer.encode("+="), classOf[mutable.Builder[_, _]].asType,
              context.rddsVar.push().invokeI(
                "apply",
                classOf[AnyRef].asType,
                context.branchKeys.getField(context.mb, marker).asType(classOf[AnyRef].asType)))
          }

          builder.invokeI("result", classOf[AnyRef].asType).cast(classOf[Seq[_]].asType)
        }, {
          getStatic(Option.getClass.asType, "MODULE$", Option.getClass.asType)
            .invokeV("apply", classOf[Option[_]].asType, {
              val sort = pushNew(classOf[ShuffleKey.SortOrdering].asType)
              sort.dup().invokeInit(
                ldc(input.getGroup.getGrouping.size), {
                  val arr = pushNewArray(Type.BOOLEAN_TYPE, input.getGroup.getOrdering.size)
                  for {
                    (ordering, i) <- input.getGroup.getOrdering.zipWithIndex
                  } {
                    arr.dup().astore(ldc(i),
                      ldc(ordering.getDirection == Group.Direction.ASCENDANT))
                  }
                  arr
                })
              sort
            }.asType(classOf[AnyRef].asType))
        },
        partitionerVar.push().asType(classOf[Partitioner].asType))
      aggregateDriver.store(context.nextLocal.getAndAdd(aggregateDriver.size))
    }
  }
}
