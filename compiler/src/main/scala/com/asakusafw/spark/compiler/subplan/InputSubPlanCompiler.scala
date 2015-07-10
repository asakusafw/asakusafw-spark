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

import java.lang.{ Boolean => JBoolean }

import scala.collection.JavaConversions._

import org.apache.hadoop.io.NullWritable
import org.objectweb.asm.Type
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.lang.compiler.hadoop.InputFormatInfoExtension
import com.asakusafw.lang.compiler.model.graph.ExternalInput
import com.asakusafw.lang.compiler.planning.SubPlan
import com.asakusafw.runtime.stage.input.TemporaryInputFormat
import com.asakusafw.spark.compiler.planning.SubPlanInfo
import com.asakusafw.spark.compiler.spi.SubPlanCompiler
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._

class InputSubPlanCompiler extends SubPlanCompiler {

  def of: SubPlanInfo.DriverType = SubPlanInfo.DriverType.INPUT

  override def instantiator: Instantiator = InputSubPlanCompiler.InputDriverInstantiator

  override def compile(
    subplan: SubPlan)(
      implicit context: SparkClientCompiler.Context): Type = {
    val subPlanInfo = subplan.getAttribute(classOf[SubPlanInfo])
    val primaryOperator = subPlanInfo.getPrimaryOperator
    assert(primaryOperator.isInstanceOf[ExternalInput],
      s"The dominant operator should be external input: ${primaryOperator}")
    val operator = primaryOperator.asInstanceOf[ExternalInput]

    val (keyType, valueType, inputFormatType, paths, extraConfigurations) =
      (if (context.jpContext.getOptions.useInputDirect) {
        Option(
          InputFormatInfoExtension
            .resolve(context.jpContext, operator.getName(), operator.getInfo()))
      } else {
        None
      }) match {
        case Some(info) =>
          (info.getKeyClass.asType, info.getValueClass.asType, info.getFormatClass.asType,
            None, Some(info.getExtraConfiguration.toMap))
        case None =>
          val inputRef = context.externalInputs.getOrElseUpdate(
            operator.getName,
            context.jpContext.addExternalInput(operator.getName, operator.getInfo))
          (classOf[NullWritable].asType,
            operator.getDataType.asType,
            classOf[TemporaryInputFormat[_]].asType,
            Some(inputRef.getPaths.toSeq.sorted),
            None)
      }

    val builder = new InputDriverClassBuilder(
      operator,
      keyType,
      valueType,
      inputFormatType,
      paths,
      extraConfigurations)(
      subPlanInfo.getLabel,
      subplan.getOutputs.toSeq)(
      context.flowId,
      context.jpContext,
      context.branchKeys,
      context.broadcastIds)

    context.jpContext.addClass(builder)
  }
}

object InputSubPlanCompiler {

  object InputDriverInstantiator extends Instantiator {

    override def newInstance(
      driverType: Type,
      subplan: SubPlan)(implicit context: Context): Var = {
      import context.mb._ // scalastyle:ignore

      val inputDriver = pushNew(driverType)
      inputDriver.dup().invokeInit(
        context.scVar.push(),
        context.hadoopConfVar.push(),
        context.broadcastsVar.push())
      inputDriver.store(context.nextLocal.getAndAdd(inputDriver.size))
    }
  }
}
