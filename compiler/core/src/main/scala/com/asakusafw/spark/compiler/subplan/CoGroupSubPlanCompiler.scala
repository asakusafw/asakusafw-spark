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
import com.asakusafw.spark.compiler.spi.SubPlanCompiler
import com.asakusafw.spark.runtime.fragment._
import com.asakusafw.spark.runtime.rdd
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._
import com.asakusafw.vocabulary.operator.CoGroup

class CoGroupSubPlanCompiler extends SubPlanCompiler {

  override def of(operator: Operator, classLoader: ClassLoader): Boolean = {
    operator match {
      case op: UserOperator =>
        op.getAnnotation.resolve(classLoader).annotationType == classOf[CoGroup]
      case _ => false
    }
  }

  override def instantiator: Instantiator = CoGroupSubPlanCompiler.CoGroupDriverInstantiator

  override def compile(subplan: SubPlan)(implicit context: Context): Type = {
    val dominant = subplan.getAttribute(classOf[DominantOperator]).getDominantOperator
    assert(dominant.isInstanceOf[UserOperator])
    val operator = dominant.asInstanceOf[UserOperator]

    val outputs = subplan.getOutputs.toSeq

    implicit val compilerContext = OperatorCompiler.Context(context.flowId, context.jpContext)
    val operators = subplan.getOperators.map { operator =>
      operator.getOriginalSerialNumber -> OperatorCompiler.compile(operator)
    }.toMap[Long, Type]

    val edges = subplan.getOperators.flatMap {
      _.getOutputs.collect {
        case output if output.getOpposites.size > 1 => output.getDataType.asType
      }
    }.map { dataType =>
      dataType -> EdgeFragmentClassBuilder.getOrCompile(context.flowId, dataType, context.jpContext)
    }.toMap

    val builder = new CoGroupDriverClassBuilder(context.flowId, classOf[AnyRef].asType) {

      override def jpContext = context.jpContext

      override def subplanOutputs: Seq[SubPlan.Output] = outputs

      override def defMethods(methodDef: MethodDef): Unit = {
        super.defMethods(methodDef)

        methodDef.newMethod("fragments", classOf[(_, _)].asType, Seq.empty) { mb =>
          import mb._
          val nextLocal = new AtomicInteger(thisVar.nextLocal)

          val fragmentBuilder = new FragmentTreeBuilder(
            mb,
            operators,
            edges,
            nextLocal)
          val fragmentVar = fragmentBuilder.build(operator)

          val outputsVar = {
            val builder = getStatic(Map.getClass.asType, "MODULE$", Map.getClass.asType)
              .invokeV("newBuilder", classOf[mutable.Builder[_, _]].asType)
            outputs.map(_.getOperator).sortBy(_.getOriginalSerialNumber).foreach { op =>
              builder.invokeI(NameTransformer.encode("+="),
                classOf[mutable.Builder[_, _]].asType,
                getStatic(Tuple2.getClass.asType, "MODULE$", Tuple2.getClass.asType).
                  invokeV("apply", classOf[(_, _)].asType,
                    ldc(op.getOriginalSerialNumber).box().asType(classOf[AnyRef].asType),
                    fragmentBuilder.vars(op.getOriginalSerialNumber).push().asType(classOf[AnyRef].asType))
                  .asType(classOf[AnyRef].asType))
            }
            val map = builder.invokeI("result", classOf[AnyRef].asType).cast(classOf[Map[_, _]].asType)
            map.store(nextLocal.getAndAdd(map.size))
          }

          `return`(
            getStatic(Tuple2.getClass.asType, "MODULE$", Tuple2.getClass.asType).
              invokeV("apply", classOf[(_, _)].asType,
                fragmentVar.push().asType(classOf[AnyRef].asType), outputsVar.push().asType(classOf[AnyRef].asType)))
        }
      }
    }

    context.jpContext.addClass(builder)
  }
}

object CoGroupSubPlanCompiler {

  object CoGroupDriverInstantiator extends Instantiator {

    override def newInstance(
      subplanType: Type,
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

      val cogroupSubplan = pushNew(subplanType)
      cogroupSubplan.dup().invokeInit(
        context.scVar.push(), {
          // Seq[(RDD[(K, _)], Option[Ordering[K]])]
          val builder = getStatic(Seq.getClass.asType, "MODULE$", Seq.getClass.asType)
            .invokeV("newBuilder", classOf[mutable.Builder[_, _]].asType)
          dominant.getInputs.foreach { input =>
            val opposites = input.getOpposites.toSet[OperatorOutput]
              .flatMap(opposite => subplan.getInputs.find(
                _.getOperator.getOriginalSerialNumber == opposite.getOwner.getOriginalSerialNumber)
                .get.getOpposites.toSeq)
              .map(_.getOperator.getSerialNumber)
              .map(context.rddVars)
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
            val ordering = pushNew0(orderingType)
            val orderingVar = ordering.store(context.nextLocal.getAndAdd(ordering.size))

            builder.invokeI(NameTransformer.encode("+="),
              classOf[mutable.Builder[_, _]].asType, {
                getStatic(Tuple2.getClass.asType, "MODULE$", Tuple2.getClass.asType)
                  .invokeV("apply", classOf[(_, _)].asType,
                    (if (opposites.size == 1) {
                      opposites.head.push()
                    } else {
                      getStatic(rdd.`package`.getClass.asType, "MODULE$", rdd.`package`.getClass.asType)
                        .invokeV("confluent", classOf[RDD[_]].asType, {
                          val builder = getStatic(Seq.getClass.asType, "MODULE$", Seq.getClass.asType)
                            .invokeV("newBuilder", classOf[mutable.Builder[_, _]].asType)
                          opposites.foreach { opposite =>
                            builder.invokeI(NameTransformer.encode("+="), classOf[mutable.Builder[_, _]].asType,
                              opposite.push().asType(classOf[AnyRef].asType))
                          }
                          builder.invokeI("result", classOf[AnyRef].asType).cast(classOf[Seq[_]].asType)
                        },
                          partitionerVar.push().asType(classOf[Partitioner].asType), {
                            getStatic(Option.getClass.asType, "MODULE$", Option.getClass.asType)
                              .invokeV("apply", classOf[Option[_]].asType,
                                orderingVar.push().asType(classOf[AnyRef].asType))
                          })
                    }).asType(classOf[AnyRef].asType), {
                      getStatic(Option.getClass.asType, "MODULE$", Option.getClass.asType)
                        .invokeV("apply", classOf[Option[_]].asType,
                          orderingVar.push().asType(classOf[AnyRef].asType))
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
      cogroupSubplan.store(context.nextLocal.getAndAdd(cogroupSubplan.size))
    }
  }
}
