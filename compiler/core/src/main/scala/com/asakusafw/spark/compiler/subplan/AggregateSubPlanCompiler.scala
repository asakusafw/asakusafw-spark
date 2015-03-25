package com.asakusafw.spark.compiler
package subplan

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.reflect.NameTransformer

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.objectweb.asm.Type

import com.asakusafw.lang.compiler.model.graph._
import com.asakusafw.lang.compiler.planning.SubPlan
import com.asakusafw.lang.compiler.planning.spark.DominantOperator
import com.asakusafw.spark.compiler.operator.{ EdgeFragmentClassBuilder, OutputFragmentClassBuilder }
import com.asakusafw.spark.compiler.operator.aggregation.AggregationClassBuilder
import com.asakusafw.spark.compiler.partitioner.GroupingPartitionerClassBuilder
import com.asakusafw.spark.compiler.spi.{ AggregationCompiler, OperatorCompiler, OperatorType, SubPlanCompiler }
import com.asakusafw.spark.runtime.fragment._
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
    assert(dominant.isInstanceOf[UserOperator])
    val operator = dominant.asInstanceOf[UserOperator]

    val operatorInputs = operator.getInputs.toSeq
    assert(operatorInputs.size == 1)
    val input = operatorInputs.head
    val inputDataModelRef = context.jpContext.getDataModelLoader.load(input.getDataType)
    val inputDataModelType = inputDataModelRef.getDeclaration.asType

    val operatorOutputs = operator.getOutputs.toSeq
    assert(operatorOutputs.size == 1)
    val output = operatorOutputs.head
    val outputDataModelRef = context.jpContext.getDataModelLoader.load(output.getDataType)
    val outputDataModelType = outputDataModelRef.getDeclaration.asType

    val outputs = subplan.getOutputs.toSeq

    val builder = new AggregateDriverClassBuilder(context.flowId, inputDataModelType, outputDataModelType) {

      override def jpContext = context.jpContext

      override def dominantOperator = operator

      override def subplanOutputs: Seq[SubPlan.Output] = outputs

      override def defMethods(methodDef: MethodDef): Unit = {
        super.defMethods(methodDef)

        methodDef.newMethod("fragments", classOf[(_, _)].asType, Seq.empty) { mb =>
          import mb._
          val nextLocal = new AtomicInteger(thisVar.nextLocal)

          val fragmentBuilder = new FragmentTreeBuilder(
            mb, nextLocal)(OperatorCompiler.Context(context.flowId, context.jpContext))
          val fragmentVar = fragmentBuilder.build(operator.getOutputs.head)
          val outputsVar = fragmentBuilder.buildOutputsVar()

          `return`(
            getStatic(Tuple2.getClass.asType, "MODULE$", Tuple2.getClass.asType).
              invokeV("apply", classOf[(_, _)].asType,
                fragmentVar.push().asType(classOf[AnyRef].asType), outputsVar.push().asType(classOf[AnyRef].asType)))
        }

        methodDef.newMethod("aggregation", classOf[Aggregation[_, _, _]].asType, Seq.empty) { mb =>
          import mb._
          val aggregationType = AggregationClassBuilder.getOrCompile(context.flowId, operator, context.jpContext)
          `return`(pushNew0(aggregationType))
        }

        defName(methodDef)
      }
    }

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

      assert(dominant.getInputs.size == 1)
      val input = dominant.getInputs.head
      val dataModelRef = context.jpContext.getDataModelLoader.load(input.getDataType)
      val properties = input.getGroup.getGrouping.map { grouping =>
        dataModelRef.findProperty(grouping).getType.asType
      }.toSeq

      val partitionerType = GroupingPartitionerClassBuilder.getOrCompile(
        context.flowId, properties, context.jpContext)
      val partitioner = pushNew(partitionerType)
      partitioner.dup().invokeInit(
        context.scVar.push()
          .invokeV("defaultParallelism", Type.INT_TYPE))
      val partitionerVar = partitioner.store(context.nextLocal.getAndAdd(partitioner.size))

      val inputRddVars = subplan.getInputs.toSet[SubPlan.Input]
        .flatMap(input => input.getOpposites.toSet[SubPlan.Output])
        .map(_.getOperator.getSerialNumber)
        .map(context.rddVars)

      val aggregateDriver = pushNew(driverType)
      aggregateDriver.dup().invokeInit(
        context.scVar.push(), {
          val builder = getStatic(Seq.getClass.asType, "MODULE$", Seq.getClass.asType)
            .invokeV("newBuilder", classOf[mutable.Builder[_, _]].asType)
          inputRddVars.foreach { rddVar =>
            builder.invokeI(NameTransformer.encode("+="), classOf[mutable.Builder[_, _]].asType,
              rddVar.push().asType(classOf[AnyRef].asType))
          }
          builder.invokeI("result", classOf[AnyRef].asType).cast(classOf[Seq[_]].asType)
        },
        partitionerVar.push().asType(classOf[Partitioner].asType))
      aggregateDriver.store(context.nextLocal.getAndAdd(aggregateDriver.size))
    }
  }
}
