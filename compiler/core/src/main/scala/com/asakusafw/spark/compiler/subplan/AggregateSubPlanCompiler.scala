/*
 * Copyright 2011-2015 Asakusa Framework Team.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.asakusafw.spark.compiler
package subplan

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.concurrent.Future
import scala.reflect.NameTransformer

import org.apache.spark.{ HashPartitioner, Partitioner }
import org.apache.spark.rdd.RDD
import org.objectweb.asm.Type
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.lang.compiler.model.graph.{ Group, UserOperator }
import com.asakusafw.lang.compiler.planning.SubPlan
import com.asakusafw.spark.compiler.operator.OperatorInfo
import com.asakusafw.spark.compiler.ordering.SortOrderingClassBuilder
import com.asakusafw.spark.compiler.planning.{ SubPlanInfo, SubPlanInputInfo }
import com.asakusafw.spark.compiler.spi.SubPlanCompiler
import com.asakusafw.spark.runtime.driver.ShuffleKey
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._

class AggregateSubPlanCompiler extends SubPlanCompiler {

  def of: SubPlanInfo.DriverType = SubPlanInfo.DriverType.AGGREGATE

  override def instantiator: Instantiator = AggregateSubPlanCompiler.AggregateDriverInstantiator

  override def compile(subplan: SubPlan)(implicit context: Context): Type = {
    val subPlanInfo = subplan.getAttribute(classOf[SubPlanInfo])
    val primaryOperator = subPlanInfo.getPrimaryOperator
    assert(primaryOperator.isInstanceOf[UserOperator],
      s"The primary operator should be user operator: ${primaryOperator}")
    val operator = primaryOperator.asInstanceOf[UserOperator]

    val operatorInfo = new OperatorInfo(operator)(context.jpContext)
    import operatorInfo._ // scalastyle:ignore

    assert(inputs.size == 1,
      s"The size of inputs should be 1: ${inputs.size}")
    assert(outputs.size == 1,
      s"The size of outputs should be 1: ${outputs.size}")

    val builder = new AggregateDriverClassBuilder(
      inputs.head.dataModelType,
      outputs.head.dataModelType,
      operator)(
      subPlanInfo.getLabel,
      subplan.getOutputs.toSeq)(
      context.flowId,
      context.jpContext,
      context.branchKeys,
      context.broadcastIds)

    context.jpContext.addClass(builder)
  }
}

object AggregateSubPlanCompiler {

  object AggregateDriverInstantiator extends Instantiator with NumPartitions {

    override def newInstance(
      driverType: Type,
      subplan: SubPlan)(implicit context: Context): Var = {
      import context.mb._ // scalastyle:ignore

      val primaryOperator =
        subplan.getAttribute(classOf[SubPlanInfo]).getPrimaryOperator.asInstanceOf[UserOperator]

      val operatorInfo = new OperatorInfo(primaryOperator)(context.jpContext)
      import operatorInfo._ // scalastyle:ignore

      assert(inputs.size == 1,
        s"The size of inputs should be 1: ${inputs.size}")
      val input = inputs.head

      val partitioner = pushNew(classOf[HashPartitioner].asType)
      partitioner.dup().invokeInit(
        if (input.getGroup.getGrouping.isEmpty) {
          ldc(1)
        } else {
          numPartitions(
            context.mb,
            context.scVar.push())(subplan.findInput(input.getOpposites.head.getOwner))
        })
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
            inputInfo = subPlanInput.getAttribute(classOf[SubPlanInputInfo])
            if inputInfo.getInputType == SubPlanInputInfo.InputType.PARTITIONED
            prevSubPlanOutput <- subPlanInput.getOpposites
            marker = prevSubPlanOutput.getOperator
          } {
            builder.invokeI(NameTransformer.encode("+="), classOf[mutable.Builder[_, _]].asType,
              context.rddsVar.push().invokeI(
                "apply",
                classOf[AnyRef].asType,
                context.branchKeys.getField(context.mb, marker)
                  .asType(classOf[AnyRef].asType))
                .cast(classOf[Future[RDD[(ShuffleKey, _)]]].asType)
                .asType(classOf[AnyRef].asType))
          }

          builder.invokeI("result", classOf[AnyRef].asType).cast(classOf[Seq[_]].asType)
        }, {
          getStatic(Option.getClass.asType, "MODULE$", Option.getClass.asType)
            .invokeV("apply", classOf[Option[_]].asType, {
              val dataModelRef = context.jpContext.getDataModelLoader.load(input.getDataType)
              pushNew0(
                SortOrderingClassBuilder.getOrCompile(
                  context.flowId,
                  input.getGroup.getGrouping.map { grouping =>
                    dataModelRef.findProperty(grouping).getType.asType
                  },
                  input.getGroup.getOrdering.map { ordering =>
                    (dataModelRef.findProperty(ordering.getPropertyName).getType.asType,
                      ordering.getDirection == Group.Direction.ASCENDANT)
                  },
                  context.jpContext))
            }.asType(classOf[AnyRef].asType))
        },
        partitionerVar.push().asType(classOf[Partitioner].asType))
      aggregateDriver.store(context.nextLocal.getAndAdd(aggregateDriver.size))
    }
  }
}
