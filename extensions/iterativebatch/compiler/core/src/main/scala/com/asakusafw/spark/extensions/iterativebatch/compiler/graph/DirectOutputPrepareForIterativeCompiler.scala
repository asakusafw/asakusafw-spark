/*
 * Copyright 2011-2017 Asakusa Framework Team.
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
package graph

import scala.collection.JavaConversions._

import org.objectweb.asm.Type

import com.asakusafw.lang.compiler.extension.directio.{ DirectFileIoModels, OutputPattern }
import com.asakusafw.lang.compiler.model.graph.ExternalOutput
import com.asakusafw.lang.compiler.planning.SubPlan
import com.asakusafw.spark.compiler._
import com.asakusafw.spark.compiler.graph.{ CacheOnce, DirectOutputSetupClassBuilder }
import com.asakusafw.spark.compiler.planning.{ IterativeInfo, SubPlanInfo }
import com.asakusafw.spark.compiler.spi.NodeCompiler

object DirectOutputPrepareForIterativeCompiler {

  def compile(
    subplan: SubPlan)(
      implicit context: NodeCompiler.Context): Type = {
    val subPlanInfo = subplan.getAttribute(classOf[SubPlanInfo])
    val primaryOperator = subPlanInfo.getPrimaryOperator
    assert(primaryOperator.isInstanceOf[ExternalOutput],
      s"The primary operator should be external output: ${primaryOperator} [${subplan}]")

    val operator = primaryOperator.asInstanceOf[ExternalOutput]
    assert(DirectFileIoModels.isSupported(operator.getInfo),
      s"The subplan is not supported: ${subplan}")

    val model = DirectFileIoModels.resolve(operator.getInfo)
    val dataModelRef = operator.getOperatorPort.dataModelRef
    val pattern = OutputPattern.compile(dataModelRef, model.getResourcePattern, model.getOrder)

    val builder = new DirectOutputPrepareForIterativeClassBuilder(
      operator)(
      pattern,
      model)(
      subplan.label) with CacheOnce

    context.addClass(builder)
  }
}
