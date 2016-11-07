/*
 * Copyright 2011-2016 Asakusa Framework Team.
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
package branching

import org.apache.hadoop.io.Writable
import org.objectweb.asm.{ Opcodes, Type }

import com.asakusafw.lang.compiler.model.graph.UserOperator
import com.asakusafw.lang.compiler.planning.SubPlan
import com.asakusafw.spark.compiler.planning.SubPlanOutputInfo
import com.asakusafw.spark.runtime.rdd.BranchKey
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._

trait Deserializer extends ClassBuilder {

  implicit def context: Deserializer.Context

  def subplanOutputs: Seq[SubPlan.Output]

  override def defMethods(methodDef: MethodDef): Unit = {
    super.defMethods(methodDef)

    methodDef.newMethod(
      "deserializerFor",
      classOf[Array[Byte] => Any].asType,
      Seq(classOf[BranchKey].asType)) { implicit mb =>
        val thisVar :: branchVar :: _ = mb.argVars

        for {
          output <- subplanOutputs
        } {
          branchVar.push().unlessNotEqual(context.branchKeys.getField(output.getOperator)) {
            val dataModelType = outputType(output)
            val outputInfo = output.getAttribute(classOf[SubPlanOutputInfo])
            val forBroadcast = outputInfo.getOutputType == SubPlanOutputInfo.OutputType.BROADCAST

            val t = DeserializerClassBuilder.getOrCompile(dataModelType, forBroadcast)
            `return`(pushNew0(t))
          }
        }
        `throw`(pushNew0(classOf[AssertionError].asType))
      }
  }

  private def outputType(output: SubPlan.Output): Type = {
    val outputInfo = output.getAttribute(classOf[SubPlanOutputInfo])
    if (outputInfo.getOutputType == SubPlanOutputInfo.OutputType.AGGREGATED) {
      val operator = outputInfo.getAggregationInfo.asInstanceOf[UserOperator]
      assert(operator.outputs.size == 1,
        s"The size of outputs should be 1: ${operator.outputs.size} [${operator}]")
      operator.outputs.head.getDataType.asType
    } else {
      output.getOperator.getDataType.asType
    }
  }
}

object Deserializer {

  trait Context
    extends CompilerContext {

    def branchKeys: BranchKeys
  }
}
