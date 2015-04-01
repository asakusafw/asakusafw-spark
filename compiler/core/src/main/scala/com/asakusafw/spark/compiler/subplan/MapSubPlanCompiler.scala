package com.asakusafw.spark.compiler
package subplan

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.reflect.{ ClassTag, NameTransformer }

import org.apache.spark.Partitioner
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.objectweb.asm.Type

import com.asakusafw.lang.compiler.model.graph._
import com.asakusafw.lang.compiler.planning.{ PlanMarker, SubPlan }
import com.asakusafw.lang.compiler.planning.spark.{ DominantOperator, PartitioningParameters }
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.spark.compiler.operator._
import com.asakusafw.spark.compiler.ordering.OrderingClassBuilder
import com.asakusafw.spark.compiler.partitioner.GroupingPartitionerClassBuilder
import com.asakusafw.spark.compiler.spi.{ OperatorCompiler, OperatorType, SubPlanCompiler }
import com.asakusafw.spark.runtime.fragment._
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._
import com.asakusafw.vocabulary.operator._

class MapSubPlanCompiler extends SubPlanCompiler {

  import MapSubPlanCompiler._

  override def support(operator: Operator)(implicit context: Context): Boolean = {
    OperatorCompiler.support(
      operator,
      OperatorType.MapType)(
        OperatorCompiler.Context(context.flowId, context.jpContext))
  }

  override def instantiator: Instantiator = MapSubPlanCompiler.MapDriverInstantiator

  override def compile(subplan: SubPlan)(implicit context: Context): Type = {
    val operator = subplan.getAttribute(classOf[DominantOperator]).getDominantOperator

    val builder = new MapDriverClassBuilder(context.flowId, operator.getInputs.head.getDataType.asType) {

      override val jpContext = context.jpContext

      override val dominantOperator = operator

      override val subplanOutputs: Seq[SubPlan.Output] = subplan.getOutputs.toSeq

      override def defMethods(methodDef: MethodDef): Unit = {
        super.defMethods(methodDef)

        methodDef.newMethod("fragments", classOf[(_, _)].asType, Seq.empty) { mb =>
          import mb._
          val nextLocal = new AtomicInteger(thisVar.nextLocal)

          val fragmentBuilder = new FragmentTreeBuilder(
            mb, nextLocal)(OperatorCompiler.Context(context.flowId, context.jpContext))
          val fragmentVar = fragmentBuilder.build(operator)
          val outputsVar = fragmentBuilder.buildOutputsVar(subplanOutputs)

          `return`(
            getStatic(Tuple2.getClass.asType, "MODULE$", Tuple2.getClass.asType).
              invokeV("apply", classOf[(_, _)].asType,
                fragmentVar.push().asType(classOf[AnyRef].asType), outputsVar.push().asType(classOf[AnyRef].asType)))
        }

        defName(methodDef)
      }
    }

    context.jpContext.addClass(builder)
  }
}

object MapSubPlanCompiler {

  val CompilableOperators: Set[Class[_]] =
    Set(classOf[Branch], classOf[Convert], classOf[Extract], classOf[Update])

  object MapDriverInstantiator extends Instantiator {

    override def newInstance(
      driverType: Type,
      subplan: SubPlan)(implicit context: Context): Var = {
      import context.mb._

      val broadcastsVar = {
        val builder = getStatic(Map.getClass.asType, "MODULE$", Map.getClass.asType)
          .invokeV("newBuilder", classOf[mutable.Builder[_, _]].asType)
        subplan.getInputs.toSet[SubPlan.Input]
          .filter(_.getOperator.getAttribute(classOf[PlanMarker]) == PlanMarker.BROADCAST)
          .foreach { input =>
            val dataModelRef = context.jpContext.getDataModelLoader.load(input.getOperator.getInput.getDataType)
            val key = input.getAttribute(classOf[PartitioningParameters]).getKey
            val groupings = key.getGrouping.toSeq.map { grouping =>
              (dataModelRef.findProperty(grouping).getType.asType, true)
            }
            val orderings = groupings ++ key.getOrdering.toSeq.map { ordering =>
              (dataModelRef.findProperty(ordering.getPropertyName).getType.asType,
                ordering.getDirection == Group.Direction.ASCENDANT)
            }

            builder.invokeI(
              NameTransformer.encode("+="),
              classOf[mutable.Builder[_, _]].asType,
              getStatic(Tuple2.getClass.asType, "MODULE$", Tuple2.getClass.asType)
                .invokeV(
                  "apply",
                  classOf[(Long, Broadcast[_])].asType,
                  ldc(input.getOperator.getOriginalSerialNumber).box().asType(classOf[AnyRef].asType),
                  thisVar.push().invokeV(
                    "broadcastAsHash",
                    classOf[Broadcast[_]].asType,
                    context.scVar.push(),
                    {
                      val builder = getStatic(Seq.getClass.asType, "MODULE$", Seq.getClass.asType)
                        .invokeV("newBuilder", classOf[mutable.Builder[_, _]].asType)

                      input.getOpposites.toSeq.map(_.getOperator.getSerialNumber).foreach { sn =>
                        builder.invokeI(NameTransformer.encode("+="), classOf[mutable.Builder[_, _]].asType,
                          context.rddsVar.push().invokeI(
                            "apply",
                            classOf[AnyRef].asType,
                            ldc(sn).box().asType(classOf[AnyRef].asType)))
                      }

                      builder.invokeI("result", classOf[AnyRef].asType).cast(classOf[Seq[_]].asType)
                    },
                    {
                      val partitionerType = GroupingPartitionerClassBuilder.getOrCompile(
                        context.flowId, groupings.map(_._1), context.jpContext)
                      val partitioner = pushNew(partitionerType)
                      partitioner.dup().invokeInit(context.scVar.push().invokeV("defaultParallelism", Type.INT_TYPE))
                      partitioner.asType(classOf[Partitioner].asType)
                    },
                    {
                      val orderingType = OrderingClassBuilder.getOrCompile(context.flowId, orderings, context.jpContext)
                      pushNew0(orderingType).asType(classOf[Ordering[_]].asType)
                    },
                    {
                      val groupingType = OrderingClassBuilder.getOrCompile(context.flowId, groupings, context.jpContext)
                      pushNew0(groupingType).asType(classOf[Ordering[_]].asType)
                    },
                    {
                      getStatic(ClassTag.getClass.asType, "MODULE$", ClassTag.getClass.asType)
                        .invokeV("apply", classOf[ClassTag[_]].asType,
                          ldc(classOf[Seq[_]].asType).asType(classOf[Class[_]].asType))
                    })
                    .asType(classOf[AnyRef].asType))
                .asType(classOf[AnyRef].asType))
          }
        val broadcasts = builder.invokeI("result", classOf[AnyRef].asType).cast(classOf[Map[_, _]].asType)
        broadcasts.store(context.nextLocal.getAndAdd(broadcasts.size))
      }

      val mapDriver = pushNew(driverType)
      mapDriver.dup().invokeInit(
        context.scVar.push(),
        context.hadoopConfVar.push(),
        broadcastsVar.push(), {
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
                  ldc(sn).box().asType(classOf[AnyRef].asType)))
            }

          builder.invokeI("result", classOf[AnyRef].asType).cast(classOf[Seq[_]].asType)
        })
      mapDriver.store(context.nextLocal.getAndAdd(mapDriver.size))
    }
  }
}
