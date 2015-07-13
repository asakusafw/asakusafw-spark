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

import org.apache.spark.rdd.RDD
import org.objectweb.asm.Type

import com.asakusafw.lang.compiler.model.graph.ExternalOutput
import com.asakusafw.lang.compiler.planning.SubPlan
import com.asakusafw.spark.compiler.planning.{ SubPlanInfo, SubPlanInputInfo }
import com.asakusafw.spark.compiler.spi.SubPlanCompiler
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._

class OutputSubPlanCompiler extends SubPlanCompiler {

  def of: SubPlanInfo.DriverType = SubPlanInfo.DriverType.OUTPUT

  override def instantiator: Instantiator = OutputSubPlanCompiler.OutputDriverInstantiator

  override def compile(
    subplan: SubPlan)(
      implicit context: SparkClientCompiler.Context): Type = {
    val subPlanInfo = subplan.getAttribute(classOf[SubPlanInfo])
    val primaryOperator = subPlanInfo.getPrimaryOperator
    assert(primaryOperator.isInstanceOf[ExternalOutput],
      s"The dominant operator should be external output: ${primaryOperator}")
    val operator = primaryOperator.asInstanceOf[ExternalOutput]

    context.jpContext.addExternalOutput(
      operator.getName, operator.getInfo,
      Seq(context.jpContext.getOptions.getRuntimeWorkingPath(s"${operator.getName}/part-*")))

    val builder =
      new OutputDriverClassBuilder(
        operator)(
        subPlanInfo.getLabel)

    context.jpContext.addClass(builder)
  }
}

object OutputSubPlanCompiler {

  object OutputDriverInstantiator extends Instantiator {

    override def newInstance(
      driverType: Type,
      subplan: SubPlan)(
        mb: MethodBuilder,
        vars: Instantiator.Vars,
        nextLocal: AtomicInteger)(
          implicit context: SparkClientCompiler.Context): Var = {
      import mb._ // scalastyle:ignore

      val outputDriver = pushNew(driverType)
      outputDriver.dup().invokeInit(
        vars.sc.push(),
        vars.hadoopConf.push(), {
          val builder = getStatic(Seq.getClass.asType, "MODULE$", Seq.getClass.asType)
            .invokeV("newBuilder", classOf[mutable.Builder[_, _]].asType)

          for {
            subPlanInput <- subplan.getInputs
            inputInfo <- Option(subPlanInput.getAttribute(classOf[SubPlanInputInfo]))
            if inputInfo.getInputType == SubPlanInputInfo.InputType.PREPARE_EXTERNAL_OUTPUT
            prevSubPlanOutput <- subPlanInput.getOpposites
            marker = prevSubPlanOutput.getOperator
          } {
            builder.invokeI(NameTransformer.encode("+="), classOf[mutable.Builder[_, _]].asType,
              vars.rdds.push().invokeI(
                "apply",
                classOf[AnyRef].asType,
                context.branchKeys.getField(mb, marker)
                  .asType(classOf[AnyRef].asType))
                .cast(classOf[Future[RDD[(_, _)]]].asType)
                .asType(classOf[AnyRef].asType))
          }

          builder.invokeI("result", classOf[AnyRef].asType).cast(classOf[Seq[_]].asType)
        },
        vars.terminators.push())
      outputDriver.store(nextLocal.getAndAdd(outputDriver.size))
    }
  }
}
