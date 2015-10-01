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

import org.objectweb.asm.Type

import com.asakusafw.lang.compiler.model.graph.{ MarkerOperator, Operator, OperatorOutput }
import com.asakusafw.lang.compiler.planning.SubPlan
import com.asakusafw.spark.compiler.operator.{
  EdgeFragmentClassBuilder,
  OutputFragmentClassBuilder
}
import com.asakusafw.spark.compiler.spi.{ OperatorCompiler, OperatorType }
import com.asakusafw.spark.runtime.driver.BroadcastId
import com.asakusafw.spark.runtime.fragment._
import com.asakusafw.spark.runtime.rdd.BranchKey
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._

class FragmentGraphBuilder(
  mb: MethodBuilder,
  broadcastsVar: Var,
  fragmentBufferSizeVar: Var,
  nextLocal: AtomicInteger)(
    implicit context: OperatorCompiler.Context)
  extends ScalaIdioms {
  import mb._ // scalastyle:ignore

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
    val fragment = operator match {
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
    }
    fragment.store(nextLocal.getAndAdd(fragment.size))
  }

  def build(output: OperatorOutput): Var = {
    if (output.getOpposites.size == 0) {
      vars.getOrElseUpdate(-1L, {
        val fragment = pushObject(mb)(StopFragment)
        fragment.store(nextLocal.getAndAdd(fragment.size))
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
        buildArray(mb, classOf[Fragment[_]].asType) { builder =>
          for {
            opposite <- opposites
          } {
            builder += opposite.push()
          }
        })
      fragment.store(nextLocal.getAndAdd(fragment.size))
    } else {
      val operator = output.getOpposites.head.getOwner
      vars.getOrElseUpdate(operator.getOriginalSerialNumber, build(operator))
    }
  }

  def buildOutputsVar(outputs: Seq[SubPlan.Output]): Var = {
    val map = buildMap(mb) { builder =>
      for {
        op <- outputs.map(_.getOperator).sortBy(_.getSerialNumber)
      } {
        builder += (
          context.branchKeys.getField(mb, op),
          vars(op.getOriginalSerialNumber).push())
      }
    }
    map.store(nextLocal.getAndAdd(map.size))
  }
}
