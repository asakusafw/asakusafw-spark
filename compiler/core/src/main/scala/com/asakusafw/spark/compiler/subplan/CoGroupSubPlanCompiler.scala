package com.asakusafw.spark.compiler
package subplan

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.reflect.{ ClassTag, NameTransformer }

import org.apache.spark.{ HashPartitioner, Partitioner, SparkContext }
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.objectweb.asm.Type

import com.asakusafw.lang.compiler.model.graph._
import com.asakusafw.lang.compiler.planning.{ PlanMarker, SubPlan }
import com.asakusafw.lang.compiler.planning.spark.{ DominantOperator, PartitioningParameters }
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.spark.compiler.operator._
import com.asakusafw.spark.compiler.ordering.OrderingClassBuilder
import com.asakusafw.spark.compiler.spi.{ OperatorCompiler, OperatorType, SubPlanCompiler }
import com.asakusafw.spark.runtime.fragment._
import com.asakusafw.spark.runtime.rdd
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._
import com.asakusafw.vocabulary.operator._

class CoGroupSubPlanCompiler extends SubPlanCompiler {

  import CoGroupSubPlanCompiler._

  override def support(operator: Operator)(implicit context: Context): Boolean = {
    OperatorCompiler.support(
      operator,
      OperatorType.CoGroupType)(
        OperatorCompiler.Context(context.flowId, context.jpContext, context.shuffleKeyTypes))
  }

  override def instantiator: Instantiator = CoGroupSubPlanCompiler.CoGroupDriverInstantiator

  override def compile(subplan: SubPlan)(implicit context: Context): Type = {
    val dominant = subplan.getAttribute(classOf[DominantOperator]).getDominantOperator
    assert(dominant.isInstanceOf[UserOperator])
    val operator = dominant.asInstanceOf[UserOperator]

    val builder = new CoGroupDriverClassBuilder(context.flowId, classOf[AnyRef].asType) {

      override val jpContext = context.jpContext

      override val shuffleKeyTypes = context.shuffleKeyTypes

      override val dominantOperator = operator

      override val subplanOutputs: Seq[SubPlan.Output] = subplan.getOutputs.toSeq

      override def defMethods(methodDef: MethodDef): Unit = {
        super.defMethods(methodDef)

        methodDef.newMethod("fragments", classOf[(_, _)].asType, Seq.empty) { mb =>
          import mb._
          val nextLocal = new AtomicInteger(thisVar.nextLocal)

          implicit val compilerContext = OperatorCompiler.Context(context.flowId, context.jpContext, context.shuffleKeyTypes)
          val fragmentBuilder = new FragmentTreeBuilder(mb, nextLocal)
          val fragmentVar = {
            val t = OperatorCompiler.compile(operator, OperatorType.CoGroupType)
            val outputs = operator.getOutputs.map(fragmentBuilder.build)
            val fragment = pushNew(t)
            fragment.dup().invokeInit(
              thisVar.push().invokeV("broadcasts", classOf[Map[Long, Broadcast[_]]].asType)
                +: outputs.map(_.push().asType(classOf[Fragment[_]].asType)): _*)
            fragment.store(nextLocal.getAndAdd(fragment.size))
          }
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

object CoGroupSubPlanCompiler {

  val CompilableOperators: Set[Class[_]] =
    Set(classOf[CoGroup], classOf[MasterBranch], classOf[MasterCheck], classOf[MasterJoin], classOf[MasterJoinUpdate])

  object CoGroupDriverInstantiator extends Instantiator {

    override def newInstance(
      driverType: Type,
      subplan: SubPlan)(implicit context: Context): Var = {
      import context.mb._

      val dominant = subplan.getAttribute(classOf[DominantOperator]).getDominantOperator

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
                      val builder = getStatic(Seq.getClass.asType, "MODULE$", Seq.getClass.asType)
                        .invokeV("newBuilder", classOf[mutable.Builder[_, _]].asType)

                      key.getOrdering.foreach { ordering =>
                        builder.invokeI(
                          NameTransformer.encode("+="),
                          classOf[mutable.Builder[_, _]].asType,
                          ldc(ordering.getDirection == Group.Direction.ASCENDANT).box().asType(classOf[AnyRef].asType))
                      }

                      builder.invokeI("result", classOf[AnyRef].asType).cast(classOf[Seq[_]].asType)
                    },
                    {
                      val partitioner = pushNew(classOf[HashPartitioner].asType)
                      partitioner.dup().invokeInit(context.scVar.push().invokeV("defaultParallelism", Type.INT_TYPE))
                      partitioner.asType(classOf[Partitioner].asType)
                    })
                    .asType(classOf[AnyRef].asType))
                .asType(classOf[AnyRef].asType))
          }
        val broadcasts = builder.invokeI("result", classOf[AnyRef].asType).cast(classOf[Map[_, _]].asType)
        broadcasts.store(context.nextLocal.getAndAdd(broadcasts.size))
      }

      val properties = dominant.getInputs.map { input =>
        val dataModelRef = context.jpContext.getDataModelLoader.load(input.getDataType)
        input.getGroup.getGrouping.map { grouping =>
          dataModelRef.findProperty(grouping).getType.asType
        }.toSeq
      }.toSet
      assert(properties.size == 1)

      val partitioner = pushNew(classOf[HashPartitioner].asType)
      partitioner.dup().invokeInit(
        context.scVar.push()
          .invokeV("defaultParallelism", Type.INT_TYPE))
      val partitionerVar = partitioner.store(context.nextLocal.getAndAdd(partitioner.size))

      val cogroupDriver = pushNew(driverType)
      cogroupDriver.dup().invokeInit(
        context.scVar.push(),
        context.hadoopConfVar.push(),
        broadcastsVar.push(), {
          // Seq[(Seq[RDD[(K, _)]], Seq[Boolean])]
          val builder = getStatic(Seq.getClass.asType, "MODULE$", Seq.getClass.asType)
            .invokeV("newBuilder", classOf[mutable.Builder[_, _]].asType)
          dominant.getInputs.foreach { input =>
            builder.invokeI(NameTransformer.encode("+="),
              classOf[mutable.Builder[_, _]].asType, {
                getStatic(Tuple2.getClass.asType, "MODULE$", Tuple2.getClass.asType)
                  .invokeV("apply", classOf[(_, _)].asType,
                    {
                      val builder = getStatic(Seq.getClass.asType, "MODULE$", Seq.getClass.asType)
                        .invokeV("newBuilder", classOf[mutable.Builder[_, _]].asType)

                      input.getOpposites.toSet[OperatorOutput]
                        .flatMap(opposite => subplan.getInputs
                          .filter { input =>
                            val marker = input.getOperator.getAttribute(classOf[PlanMarker])
                            marker == PlanMarker.CHECKPOINT || marker == PlanMarker.GATHER
                          }.find(
                            _.getOperator.getOriginalSerialNumber == opposite.getOwner.getOriginalSerialNumber)
                          .get.getOpposites.toSeq)
                        .map(_.getOperator.getSerialNumber)
                        .foreach { sn =>
                          builder.invokeI(NameTransformer.encode("+="), classOf[mutable.Builder[_, _]].asType,
                            context.rddsVar.push().invokeI(
                              "apply",
                              classOf[AnyRef].asType,
                              ldc(sn).box().asType(classOf[AnyRef].asType)))
                        }

                      builder.invokeI("result", classOf[AnyRef].asType).cast(classOf[Seq[_]].asType)
                    }.asType(classOf[AnyRef].asType),
                    {
                      val builder = getStatic(Seq.getClass.asType, "MODULE$", Seq.getClass.asType)
                        .invokeV("newBuilder", classOf[mutable.Builder[_, _]].asType)

                      input.getGroup.getOrdering.foreach { ordering =>
                        builder.invokeI(
                          NameTransformer.encode("+="),
                          classOf[mutable.Builder[_, _]].asType,
                          ldc(ordering.getDirection == Group.Direction.ASCENDANT).box().asType(classOf[AnyRef].asType))
                      }

                      builder.invokeI("result", classOf[AnyRef].asType).cast(classOf[Seq[_]].asType)
                        .asType(classOf[AnyRef].asType)
                    }).asType(classOf[AnyRef].asType)
              })
          }
          builder.invokeI("result", classOf[AnyRef].asType).cast(classOf[Seq[_]].asType)
        }, {
          // Partitioner
          partitionerVar.push().asType(classOf[Partitioner].asType)
        })
      cogroupDriver.store(context.nextLocal.getAndAdd(cogroupDriver.size))
    }
  }
}
