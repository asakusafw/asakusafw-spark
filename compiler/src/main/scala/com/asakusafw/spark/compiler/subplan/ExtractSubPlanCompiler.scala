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

import org.apache.spark.rdd.RDD
import org.objectweb.asm.Type
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.lang.compiler.planning.SubPlan
import com.asakusafw.spark.compiler.planning.{ SubPlanInfo, SubPlanInputInfo }
import com.asakusafw.spark.compiler.spi.SubPlanCompiler
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._

class ExtractSubPlanCompiler extends SubPlanCompiler {

  def of: SubPlanInfo.DriverType = SubPlanInfo.DriverType.EXTRACT

  override def instantiator: Instantiator = ExtractSubPlanCompiler.ExtractDriverInstantiator

  override def compile(
    subplan: SubPlan)(
      implicit context: SparkClientCompiler.Context): Type = {
    val subPlanInfo = subplan.getAttribute(classOf[SubPlanInfo])

    val inputs = subplan.getInputs.toSet[SubPlan.Input]
    assert(inputs.size == 1)

    val marker = inputs.head.getOperator

    val builder = new ExtractDriverClassBuilder(
      marker)(
      subPlanInfo.getLabel,
      subplan.getOutputs.toSeq)(
      context.flowId,
      context.jpContext,
      context.branchKeys,
      context.broadcastIds)

    context.jpContext.addClass(builder)
  }
}

object ExtractSubPlanCompiler {

  object ExtractDriverInstantiator extends Instantiator {

    override def newInstance(
      driverType: Type,
      subplan: SubPlan)(implicit context: Context): Var = {
      import context.mb._ // scalastyle:ignore

      val extractDriver = pushNew(driverType)
      extractDriver.dup().invokeInit(
        context.scVar.push(),
        context.hadoopConfVar.push(),
        context.broadcastsVar.push(), {
          val builder = getStatic(Seq.getClass.asType, "MODULE$", Seq.getClass.asType)
            .invokeV("newBuilder", classOf[mutable.Builder[_, _]].asType)

          for {
            subPlanInput <- subplan.getInputs.toSet[SubPlan.Input]
            inputInfo <- Option(subPlanInput.getAttribute(classOf[SubPlanInputInfo]))
            if inputInfo.getInputType == SubPlanInputInfo.InputType.DONT_CARE
            prevSubPlanOutput <- subPlanInput.getOpposites.toSeq
            marker = prevSubPlanOutput.getOperator
          } {
            builder.invokeI(NameTransformer.encode("+="), classOf[mutable.Builder[_, _]].asType,
              context.rddsVar.push().invokeI(
                "apply",
                classOf[AnyRef].asType,
                context.branchKeys.getField(context.mb, marker)
                  .asType(classOf[AnyRef].asType))
                .cast(classOf[Future[RDD[(_, _)]]].asType)
                .asType(classOf[AnyRef].asType))
          }

          builder.invokeI("result", classOf[AnyRef].asType).cast(classOf[Seq[_]].asType)
        })
      extractDriver.store(context.nextLocal.getAndAdd(extractDriver.size))
    }
  }
}
