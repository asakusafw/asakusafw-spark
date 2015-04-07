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
import com.asakusafw.spark.compiler.spi.{ OperatorCompiler, OperatorType, SubPlanCompiler }
import com.asakusafw.spark.runtime.driver.BranchKey
import com.asakusafw.spark.runtime.fragment._
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._

class CoGroupSubPlanCompiler extends SubPlanCompiler {

  import CoGroupSubPlanCompiler._

  override def support(operator: Operator)(implicit context: Context): Boolean = {
    OperatorCompiler.support(
      operator,
      OperatorType.CoGroupType)(
        OperatorCompiler.Context(
          flowId = context.flowId,
          jpContext = context.jpContext,
          branchKeys = context.branchKeys,
          shuffleKeyTypes = context.shuffleKeyTypes))
  }

  override def instantiator: Instantiator = CoGroupSubPlanCompiler.CoGroupDriverInstantiator

  override def compile(subplan: SubPlan)(implicit context: Context): Type = {
    val dominant = subplan.getAttribute(classOf[DominantOperator]).getDominantOperator
    assert(dominant.isInstanceOf[UserOperator],
      s"The dominant operator should be user operator: ${dominant}")
    val operator = dominant.asInstanceOf[UserOperator]

    val builder = new CoGroupDriverClassBuilder(context.flowId, classOf[AnyRef].asType) {

      override val jpContext = context.jpContext

      override val shuffleKeyTypes = context.shuffleKeyTypes

      override val branchKeys: BranchKeysClassBuilder = context.branchKeys

      override val dominantOperator = operator

      override val subplanOutputs: Seq[SubPlan.Output] = subplan.getOutputs.toSeq

      override def defMethods(methodDef: MethodDef): Unit = {
        super.defMethods(methodDef)

        methodDef.newMethod("fragments", classOf[(_, _)].asType, Seq.empty) { mb =>
          import mb._
          val nextLocal = new AtomicInteger(thisVar.nextLocal)

          implicit val compilerContext =
            OperatorCompiler.Context(
              flowId = context.flowId,
              jpContext = context.jpContext,
              branchKeys = context.branchKeys,
              shuffleKeyTypes = context.shuffleKeyTypes)
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

  object CoGroupDriverInstantiator extends Instantiator {

    override def newInstance(
      driverType: Type,
      subplan: SubPlan)(implicit context: Context): Var = {
      import context.mb._

      val dominant = subplan.getAttribute(classOf[DominantOperator]).getDominantOperator

      val operatorInfo = new OperatorInfo(dominant)(context.jpContext)
      import operatorInfo._

      val properties = inputs.map { input =>
        val dataModelRef = input.dataModelRef
        input.getGroup.getGrouping.map { grouping =>
          dataModelRef.findProperty(grouping).getType.asType
        }.toSeq
      }.toSet
      assert(properties.size == 1,
        s"The grouping of all inputs should be the same: ${
          properties.map(_.mkString("(", ",", ")")).mkString("(", ",", ")")
        }")

      val partitioner = pushNew(classOf[HashPartitioner].asType)
      partitioner.dup().invokeInit(
        context.scVar.push()
          .invokeV("defaultParallelism", Type.INT_TYPE))
      val partitionerVar = partitioner.store(context.nextLocal.getAndAdd(partitioner.size))

      val cogroupDriver = pushNew(driverType)
      cogroupDriver.dup().invokeInit(
        context.scVar.push(),
        context.hadoopConfVar.push(),
        context.broadcastsVar.push(), {
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
                              getStatic(
                                context.branchKeys.thisType,
                                context.branchKeys.getField(sn),
                                classOf[BranchKey].asType).asType(classOf[AnyRef].asType)))
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
