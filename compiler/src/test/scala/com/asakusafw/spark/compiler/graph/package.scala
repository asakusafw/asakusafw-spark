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

import scala.collection.JavaConversions._
import scala.reflect.{ classTag, ClassTag }

import com.asakusafw.lang.compiler.model.graph.MarkerOperator
import com.asakusafw.lang.compiler.planning.SubPlan

package object graph {

  implicit class AugmentedSubPlan(val subplan: SubPlan) extends AnyVal {

    def putAttr[A: ClassTag](attribute: SubPlan => A): SubPlan = {
      subplan.putAttribute(classTag[A].runtimeClass.asInstanceOf[Class[A]], attribute(subplan))
      subplan
    }

    def findIn(marker: MarkerOperator): SubPlan.Input = {
      subplan.getInputs
        .find(_.getOperator.getOriginalSerialNumber == marker.getOriginalSerialNumber).orNull
    }

    def findOut(marker: MarkerOperator): SubPlan.Output = {
      subplan.getOutputs
        .find(_.getOperator.getOriginalSerialNumber == marker.getOriginalSerialNumber).orNull
    }
  }

  implicit class AugmentedSubPlanInput(val input: SubPlan.Input) extends AnyVal {

    def putAttr[A: ClassTag](attribute: SubPlan.Input => A): SubPlan.Input = {
      input.putAttribute(classTag[A].runtimeClass.asInstanceOf[Class[A]], attribute(input))
      input
    }
  }

  implicit class AugmentedSubPlanOutput(val output: SubPlan.Output) extends AnyVal {

    def putAttr[A: ClassTag](attribute: SubPlan.Output => A): SubPlan.Output = {
      output.putAttribute(classTag[A].runtimeClass.asInstanceOf[Class[A]], attribute(output))
      output
    }
  }
}
