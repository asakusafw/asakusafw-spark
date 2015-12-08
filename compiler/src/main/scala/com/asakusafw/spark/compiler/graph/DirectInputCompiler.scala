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
package graph

import scala.collection.JavaConversions._

import org.apache.hadoop.io.NullWritable
import org.objectweb.asm.Type

import com.asakusafw.lang.compiler.model.graph.ExternalInput
import com.asakusafw.lang.compiler.planning.SubPlan
import com.asakusafw.runtime.stage.input.TemporaryInputFormat
import com.asakusafw.spark.compiler.planning.SubPlanInfo
import com.asakusafw.spark.compiler.spi.NodeCompiler

class DirectInputCompiler extends NodeCompiler {

  override def support(
    subplan: SubPlan)(
      implicit context: NodeCompiler.Context): Boolean = {
    subplan.getAttribute(classOf[SubPlanInfo]).getDriverType == SubPlanInfo.DriverType.INPUT && {
      val subPlanInfo = subplan.getAttribute(classOf[SubPlanInfo])
      val primaryOperator = subPlanInfo.getPrimaryOperator
      if (primaryOperator.isInstanceOf[ExternalInput]) {
        val operator = primaryOperator.asInstanceOf[ExternalInput]
        val inputFormatInfo = context.getInputFormatInfo(operator.getName, operator.getInfo)
        context.options.useInputDirect && inputFormatInfo.isDefined
      } else {
        false
      }
    }
  }

  override def instantiator: Instantiator = InputInstantiator

  override def compile(
    subplan: SubPlan)(
      implicit context: NodeCompiler.Context): Type = {
    assert(support(subplan), s"The subplan is not supported: ${subplan}")

    val subPlanInfo = subplan.getAttribute(classOf[SubPlanInfo])
    val primaryOperator = subPlanInfo.getPrimaryOperator

    assert(primaryOperator.isInstanceOf[ExternalInput],
      s"The primary operator should be external input: ${primaryOperator} [${subplan}]")
    val operator = primaryOperator.asInstanceOf[ExternalInput]

    val inputFormatInfo = context.getInputFormatInfo(operator.getName, operator.getInfo).get

    val builder =
      new DirectInputClassBuilder(
        operator,
        inputFormatInfo.getFormatClass.asType,
        inputFormatInfo.getKeyClass.asType,
        inputFormatInfo.getValueClass.asType,
        inputFormatInfo.getExtraConfiguration.toMap)(
        subPlanInfo.getLabel,
        subplan.getOutputs.toSeq) with ComputeOnce

    context.addClass(builder)
  }
}
