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

import org.apache.hadoop.io.NullWritable
import org.objectweb.asm.Type

import com.asakusafw.lang.compiler.model.graph.ExternalInput
import com.asakusafw.lang.compiler.planning.SubPlan
import com.asakusafw.runtime.stage.input.TemporaryInputFormat
import com.asakusafw.spark.compiler.planning.SubPlanInfo
import com.asakusafw.spark.compiler.spi.SubPlanCompiler
import com.asakusafw.spark.tools.asm._

class InputSubPlanCompiler extends SubPlanCompiler {

  def of: SubPlanInfo.DriverType = SubPlanInfo.DriverType.INPUT

  override def instantiator: Instantiator = InputDriverInstantiator

  override def compile(
    subplan: SubPlan)(
      implicit context: SubPlanCompiler.Context): Type = {
    val subPlanInfo = subplan.getAttribute(classOf[SubPlanInfo])
    val primaryOperator = subPlanInfo.getPrimaryOperator
    assert(primaryOperator.isInstanceOf[ExternalInput],
      s"The primary operator should be external input: ${primaryOperator} [${subplan}]")
    val operator = primaryOperator.asInstanceOf[ExternalInput]

    val (inputFormatType, keyType, valueType, paths, extraConfigurations) =
      (if (context.options.useInputDirect) {
        context.getInputFormatInfo(operator.getName, operator.getInfo)
      } else {
        None
      }) match {
        case Some(info) =>
          (info.getFormatClass.asType, info.getKeyClass.asType, info.getValueClass.asType,
            None, Some(info.getExtraConfiguration.toMap))
        case None =>
          val inputRef = context.addExternalInput(operator.getName, operator.getInfo)
          (classOf[TemporaryInputFormat[_]].asType,
            classOf[NullWritable].asType,
            operator.getDataType.asType,
            Some(inputRef.getPaths.toSeq.sorted),
            None)
      }

    val builder =
      new InputDriverClassBuilder(
        operator,
        inputFormatType,
        keyType,
        valueType,
        paths,
        extraConfigurations)(
        subPlanInfo.getLabel,
        subplan.getOutputs.toSeq)

    context.addClass(builder)
  }
}
