package com.asakusafw.spark.compiler
package subplan

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.reflect.NameTransformer

import org.apache.spark.{ Partitioner, SparkContext }
import org.apache.spark.rdd.RDD
import org.objectweb.asm.Type

import com.asakusafw.lang.compiler.model.graph._
import com.asakusafw.lang.compiler.planning.SubPlan
import com.asakusafw.lang.compiler.planning.spark.{ DominantOperator, PartitioningParameters }
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.spark.compiler.operator._
import com.asakusafw.spark.compiler.ordering.OrderingClassBuilder
import com.asakusafw.spark.compiler.partitioner.GroupingPartitionerClassBuilder
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
        OperatorCompiler.Context(context.flowId, context.jpContext))
  }

  override def instantiator: Instantiator = CoGroupSubPlanCompiler.CoGroupDriverInstantiator

  override def compile(subplan: SubPlan)(implicit context: Context): Type = {
    val dominant = subplan.getAttribute(classOf[DominantOperator]).getDominantOperator
    assert(dominant.isInstanceOf[UserOperator])
    val operator = dominant.asInstanceOf[UserOperator]

    val outputs = subplan.getOutputs.toSeq

    val builder = new CoGroupDriverClassBuilder(context.flowId, classOf[AnyRef].asType) {

      override def jpContext = context.jpContext

      override def dominantOperator = operator

      override def subplanOutputs: Seq[SubPlan.Output] = outputs

      override def defMethods(methodDef: MethodDef): Unit = {
        super.defMethods(methodDef)

        methodDef.newMethod("fragments", classOf[(_, _)].asType, Seq.empty) { mb =>
          import mb._
          val nextLocal = new AtomicInteger(thisVar.nextLocal)

          implicit val compilerContext = OperatorCompiler.Context(context.flowId, context.jpContext)
          val fragmentBuilder = new FragmentTreeBuilder(mb, nextLocal)
          val fragmentVar = {
            val t = OperatorCompiler.compile(operator, OperatorType.CoGroupType)
            val outputs = operator.getOutputs.map(fragmentBuilder.build)
            val fragment = pushNew(t)
            fragment.dup().invokeInit(outputs.map(_.push().asType(classOf[Fragment[_]].asType)): _*)
            fragment.store(nextLocal.getAndAdd(fragment.size))
          }
          val outputsVar = fragmentBuilder.buildOutputsVar()

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

      val properties = dominant.getInputs.map { input =>
        val dataModelRef = context.jpContext.getDataModelLoader.load(input.getDataType)
        input.getGroup.getGrouping.map { grouping =>
          dataModelRef.findProperty(grouping).getType.asType
        }.toSeq
      }.toSet
      assert(properties.size == 1)
      val partitionerType = GroupingPartitionerClassBuilder.getOrCompile(
        context.flowId, properties.head, context.jpContext)
      val orderingType = OrderingClassBuilder.getOrCompile(
        context.flowId, properties.head.map((_, true)), context.jpContext)

      val partitioner = pushNew(partitionerType)
      partitioner.dup().invokeInit(
        context.scVar.push()
          .invokeV("defaultParallelism", Type.INT_TYPE))
      val partitionerVar = partitioner.store(context.nextLocal.getAndAdd(partitioner.size))

      val cogroupDriver = pushNew(driverType)
      cogroupDriver.dup().invokeInit(
        context.scVar.push(), {
          // Seq[(Seq[RDD[(K, _)]], Option[Ordering[K]])]
          val builder = getStatic(Seq.getClass.asType, "MODULE$", Seq.getClass.asType)
            .invokeV("newBuilder", classOf[mutable.Builder[_, _]].asType)
          dominant.getInputs.foreach { input =>
            builder.invokeI(NameTransformer.encode("+="),
              classOf[mutable.Builder[_, _]].asType, {
                getStatic(Tuple2.getClass.asType, "MODULE$", Tuple2.getClass.asType)
                  .invokeV("apply", classOf[(_, _)].asType,
                    {
                      val oppositeRddVars = input.getOpposites.toSet[OperatorOutput]
                        .flatMap(opposite => subplan.getInputs.find(
                          _.getOperator.getOriginalSerialNumber == opposite.getOwner.getOriginalSerialNumber)
                          .get.getOpposites.toSeq)
                        .map(_.getOperator.getSerialNumber)
                        .map(context.rddVars)

                      val builder = getStatic(Seq.getClass.asType, "MODULE$", Seq.getClass.asType)
                        .invokeV("newBuilder", classOf[mutable.Builder[_, _]].asType)
                      oppositeRddVars.foreach { rddVar =>
                        builder.invokeI(NameTransformer.encode("+="), classOf[mutable.Builder[_, _]].asType,
                          rddVar.push().asType(classOf[AnyRef].asType))
                      }
                      builder.invokeI("result", classOf[AnyRef].asType).cast(classOf[Seq[_]].asType)
                    }.asType(classOf[AnyRef].asType),
                    {
                      val orderings = {
                        val dataModelRef = context.jpContext.getDataModelLoader.load(input.getDataType)
                        val group = input.getGroup
                        group.getGrouping.map { grouping =>
                          (dataModelRef.findProperty(grouping).getType.asType, true)
                        } ++
                          group.getOrdering.map { ordering =>
                            (dataModelRef.findProperty(ordering.getPropertyName).getType.asType,
                              ordering.getDirection == Group.Direction.ASCENDANT)
                          }
                      }.toSeq
                      val orderingType = OrderingClassBuilder.getOrCompile(context.flowId, orderings, context.jpContext)

                      getStatic(Option.getClass.asType, "MODULE$", Option.getClass.asType)
                        .invokeV("apply", classOf[Option[_]].asType,
                          pushNew0(orderingType).asType(classOf[AnyRef].asType))
                        .asType(classOf[AnyRef].asType)
                    }).asType(classOf[AnyRef].asType)
              })
          }
          builder.invokeI("result", classOf[AnyRef].asType).cast(classOf[Seq[_]].asType)
        }, {
          // Partitioner
          partitionerVar.push().asType(classOf[Partitioner].asType)
        }, {
          // Ordering
          pushNew0(orderingType).asType(classOf[Ordering[_]].asType)
        })
      cogroupDriver.store(context.nextLocal.getAndAdd(cogroupDriver.size))
    }
  }
}
