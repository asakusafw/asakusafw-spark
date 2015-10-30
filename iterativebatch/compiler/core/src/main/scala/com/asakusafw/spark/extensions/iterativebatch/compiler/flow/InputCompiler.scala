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
package com.asakusafw.spark.extensions.iterativebatch.compiler
package flow

import scala.collection.JavaConversions._

import org.apache.hadoop.io.NullWritable
import org.objectweb.asm.Type

import com.asakusafw.lang.compiler.model.graph.ExternalInput
import com.asakusafw.lang.compiler.planning.SubPlan
import com.asakusafw.runtime.stage.input.TemporaryInputFormat
import com.asakusafw.spark.compiler.`package`._
import com.asakusafw.spark.compiler.planning.SubPlanInfo
import com.asakusafw.spark.compiler.subplan.InputDriverClassBuilder

import com.asakusafw.spark.extensions.iterativebatch.compiler.spi.NodeCompiler

class InputCompiler extends NodeCompiler {

  def of: SubPlanInfo.DriverType = SubPlanInfo.DriverType.INPUT

  override def compile(
    subplan: SubPlan)(
      implicit context: NodeCompiler.Context): Type = {
    val subPlanInfo = subplan.getAttribute(classOf[SubPlanInfo])
    val primaryOperator = subPlanInfo.getPrimaryOperator
    assert(primaryOperator.isInstanceOf[ExternalInput],
      s"The dominant operator should be external input: ${primaryOperator}")
    val operator = primaryOperator.asInstanceOf[ExternalInput]

    val inputFormatInfo = context.getInputFormatInfo(operator.getName, operator.getInfo)

    val builder = if (context.options.useInputDirect && inputFormatInfo.isDefined) {
      val info = inputFormatInfo.get
      new DirectInputClassBuilder(
        operator,
        info.getFormatClass.asType,
        info.getKeyClass.asType,
        info.getValueClass.asType,
        info.getExtraConfiguration.toMap)(
        subPlanInfo.getLabel,
        subplan.getOutputs.toSeq)
    } else {
      val inputRef = context.addExternalInput(operator.getName, operator.getInfo)
      new TemporaryInputClassBuilder(
        operator,
        operator.getDataType.asType,
        inputRef.getPaths.toSeq.sorted)(
        subPlanInfo.getLabel,
        subplan.getOutputs.toSeq)
    }

    context.addClass(builder)
  }
}
