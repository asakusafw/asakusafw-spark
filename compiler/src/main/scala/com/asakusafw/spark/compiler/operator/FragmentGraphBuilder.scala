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
package operator

import scala.collection.JavaConversions._
import scala.collection.mutable

import org.objectweb.asm.Type

import com.asakusafw.lang.compiler.model.graph.{ MarkerOperator, Operator, OperatorOutput }
import com.asakusafw.lang.compiler.planning.SubPlan
import com.asakusafw.spark.compiler.spi.{ OperatorCompiler, OperatorType }
import com.asakusafw.spark.runtime.fragment.{ Fragment, StopFragment }
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._
import com.asakusafw.spark.tools.asm4s._

class FragmentGraphBuilder(
  broadcastsVar: Var,
  fragmentBufferSizeVar: Var)(
    implicit mb: MethodBuilder,
    context: OperatorCompiler.Context) {

  val operatorFragmentTypes: mutable.Map[Long, Type] = mutable.Map.empty
  val edgeFragmentTypes: mutable.Map[Type, Type] = mutable.Map.empty

  val vars: mutable.Map[Long, Var] = mutable.Map.empty

  def build(operator: Operator): Var = {
    val t = operatorFragmentTypes.getOrElseUpdate(
      operator.getOriginalSerialNumber, {
        operator match {
          case marker: MarkerOperator =>
            OutputFragmentClassBuilder.getOrCompile(marker.getInput.getDataType.asType)
          case operator: Operator =>
            OperatorCompiler.compile(operator, OperatorType.ExtractType)
        }
      })
    (operator match {
      case marker: MarkerOperator =>
        val fragment = pushNew(t)
        fragment.dup().invokeInit(fragmentBufferSizeVar.push())
        fragment
      case _ =>
        val outputs = operator.getOutputs.map(build)
        val fragment = pushNew(t)
        fragment.dup().invokeInit(
          broadcastsVar.push()
            +: outputs.map(_.push().asType(classOf[Fragment[_]].asType)): _*)
        fragment
    }).store()
  }

  def build(output: OperatorOutput): Var = {
    if (output.getOpposites.size == 0) {
      vars.getOrElseUpdate(-1L, {
        pushObject(StopFragment).store()
      })
    } else if (output.getOpposites.size > 1) {
      val opposites = output.getOpposites.toSeq.map(_.getOwner).map { operator =>
        vars.getOrElseUpdate(operator.getOriginalSerialNumber, build(operator))
      }
      val fragment = pushNew(
        edgeFragmentTypes.getOrElseUpdate(
          output.getDataType.asType,
          EdgeFragmentClassBuilder.getOrCompile(output.getDataType.asType)))
      fragment.dup().invokeInit(
        buildArray(classOf[Fragment[_]].asType) { builder =>
          for {
            opposite <- opposites
          } {
            builder += opposite.push()
          }
        })
      fragment.store()
    } else {
      val operator = output.getOpposites.head.getOwner
      vars.getOrElseUpdate(operator.getOriginalSerialNumber, build(operator))
    }
  }

  def buildOutputsVar(outputs: Seq[SubPlan.Output]): Var = {
    (buildMap { builder =>
      for {
        op <- outputs.map(_.getOperator).sortBy(_.getSerialNumber)
      } {
        builder += (
          context.branchKeys.getField(op),
          vars(op.getOriginalSerialNumber).push())
      }
    }).store()
  }
}
