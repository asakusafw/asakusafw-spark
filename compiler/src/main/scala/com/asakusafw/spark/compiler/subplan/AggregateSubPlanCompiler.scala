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

import java.util.concurrent.atomic.AtomicInteger

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
import com.asakusafw.spark.compiler.ordering.SortOrderingClassBuilder
import com.asakusafw.spark.compiler.planning.{ SubPlanInfo, SubPlanInputInfo }
import com.asakusafw.spark.compiler.spi.SubPlanCompiler
import com.asakusafw.spark.runtime.driver.ShuffleKey
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._

class AggregateSubPlanCompiler extends SubPlanCompiler {

  def of: SubPlanInfo.DriverType = SubPlanInfo.DriverType.AGGREGATE

  override def instantiator: Instantiator = AggregateSubPlanCompiler.AggregateDriverInstantiator

  override def compile(
    subplan: SubPlan)(
      implicit context: SparkClientCompiler.Context): Type = {
    val subPlanInfo = subplan.getAttribute(classOf[SubPlanInfo])
    val primaryOperator = subPlanInfo.getPrimaryOperator
    assert(primaryOperator.isInstanceOf[UserOperator],
      s"The primary operator should be user operator: ${primaryOperator}")
    val operator = primaryOperator.asInstanceOf[UserOperator]

    assert(operator.inputs.size == 1,
      s"The size of inputs should be 1: ${operator.inputs.size}")
    assert(operator.outputs.size == 1,
      s"The size of outputs should be 1: ${operator.outputs.size}")

    val builder =
      new AggregateDriverClassBuilder(
        operator.inputs.head.dataModelType,
        operator.outputs.head.dataModelType,
        operator)(
        subPlanInfo.getLabel,
        subplan.getOutputs.toSeq)

    context.jpContext.addClass(builder)
  }
}

object AggregateSubPlanCompiler {

  object AggregateDriverInstantiator
    extends Instantiator
    with NumPartitions
    with ScalaIdioms {

    override def newInstance(
      driverType: Type,
      subplan: SubPlan)(
        mb: MethodBuilder,
        vars: Instantiator.Vars,
        nextLocal: AtomicInteger)(
          implicit context: SparkClientCompiler.Context): Var = {
      import mb._ // scalastyle:ignore

      val primaryOperator =
        subplan.getAttribute(classOf[SubPlanInfo]).getPrimaryOperator.asInstanceOf[UserOperator]

      assert(primaryOperator.inputs.size == 1,
        s"The size of inputs should be 1: ${primaryOperator.inputs.size}")
      val input = primaryOperator.inputs.head

      val partitioner = pushNew(classOf[HashPartitioner].asType)
      partitioner.dup().invokeInit(
        if (input.getGroup.getGrouping.isEmpty) {
          ldc(1)
        } else {
          numPartitions(
            mb,
            vars.sc.push())(subplan.findInput(input.getOpposites.head.getOwner))
        })
      val partitionerVar = partitioner.store(nextLocal.getAndAdd(partitioner.size))

      val aggregateDriver = pushNew(driverType)
      aggregateDriver.dup().invokeInit(
        vars.sc.push(),
        vars.hadoopConf.push(),
        vars.broadcasts.push(), {
          val builder = pushObject(mb)(Seq)
            .invokeV("newBuilder", classOf[mutable.Builder[_, _]].asType)

          for {
            subPlanInput <- subplan.getInputs
            inputInfo = subPlanInput.getAttribute(classOf[SubPlanInputInfo])
            if inputInfo.getInputType == SubPlanInputInfo.InputType.PARTITIONED
            prevSubPlanOutput <- subPlanInput.getOpposites
            marker = prevSubPlanOutput.getOperator
          } {
            builder.invokeI(NameTransformer.encode("+="), classOf[mutable.Builder[_, _]].asType,
              vars.rdds.push().invokeI(
                "apply",
                classOf[AnyRef].asType,
                context.branchKeys.getField(mb, marker)
                  .asType(classOf[AnyRef].asType))
                .cast(classOf[Future[RDD[(ShuffleKey, _)]]].asType)
                .asType(classOf[AnyRef].asType))
          }

          builder.invokeI("result", classOf[AnyRef].asType).cast(classOf[Seq[_]].asType)
        }, {
          pushObject(mb)(Option)
            .invokeV("apply", classOf[Option[_]].asType, {
              val dataModelRef = context.jpContext.getDataModelLoader.load(input.getDataType)
              pushNew0(
                SortOrderingClassBuilder.getOrCompile(
                  input.getGroup.getGrouping.map { grouping =>
                    dataModelRef.findProperty(grouping).getType.asType
                  },
                  input.getGroup.getOrdering.map { ordering =>
                    (dataModelRef.findProperty(ordering.getPropertyName).getType.asType,
                      ordering.getDirection == Group.Direction.ASCENDANT)
                  }))
            }.asType(classOf[AnyRef].asType))
        },
        partitionerVar.push().asType(classOf[Partitioner].asType))
      aggregateDriver.store(nextLocal.getAndAdd(aggregateDriver.size))
    }
  }
}
