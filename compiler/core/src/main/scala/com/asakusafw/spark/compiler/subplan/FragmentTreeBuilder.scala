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
import scala.reflect.NameTransformer

import org.apache.spark.broadcast.Broadcast
import org.objectweb.asm.Type

import com.asakusafw.lang.compiler.model.graph._
import com.asakusafw.lang.compiler.planning.SubPlan
import com.asakusafw.spark.compiler.operator.{ EdgeFragmentClassBuilder, OutputFragmentClassBuilder }
import com.asakusafw.spark.compiler.spi.{ OperatorCompiler, OperatorType }
import com.asakusafw.spark.runtime.driver.BroadcastId
import com.asakusafw.spark.runtime.fragment._
import com.asakusafw.spark.runtime.rdd.BranchKey
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._

class FragmentTreeBuilder(
    mb: MethodBuilder,
    broadcastsVar: Var,
    nextLocal: AtomicInteger)(implicit context: OperatorCompiler.Context) {
  import mb._ // scalastyle:ignore

  val operatorFragmentTypes: mutable.Map[Long, Type] = mutable.Map.empty
  val edgeFragmentTypes: mutable.Map[Type, Type] = mutable.Map.empty

  val vars: mutable.Map[Long, Var] = mutable.Map.empty

  def build(operator: Operator): Var = {
    val t = operatorFragmentTypes.getOrElseUpdate(
      operator.getOriginalSerialNumber, {
        operator match {
          case marker: MarkerOperator =>
            OutputFragmentClassBuilder.getOrCompile(context.flowId, marker.getInput.getDataType.asType, context.jpContext)
          case operator =>
            OperatorCompiler.compile(operator, OperatorType.MapType)
        }
      })
    val fragment = operator match {
      case marker: MarkerOperator =>
        pushNew0(t)
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
        val fragment = getStatic(StopFragment.getClass.asType, "MODULE$", StopFragment.getClass.asType)
        fragment.store(nextLocal.getAndAdd(fragment.size))
      })
    } else if (output.getOpposites.size > 1) {
      val opposites = output.getOpposites.toSeq.map(_.getOwner).map { operator =>
        vars.getOrElseUpdate(operator.getOriginalSerialNumber, build(operator))
      }
      val fragment = pushNew(
        edgeFragmentTypes.getOrElseUpdate(
          output.getDataType.asType, {
            EdgeFragmentClassBuilder.getOrCompile(context.flowId, output.getDataType.asType, context.jpContext)
          }))
      fragment.dup().invokeInit({
        val arr = pushNewArray(classOf[Fragment[_]].asType, output.getOpposites.size)
        opposites.zipWithIndex.foreach {
          case (opposite, i) =>
            arr.dup().astore(ldc(i), opposite.push())
        }
        arr
      })
      fragment.store(nextLocal.getAndAdd(fragment.size))
    } else {
      val operator = output.getOpposites.head.getOwner
      vars.getOrElseUpdate(operator.getOriginalSerialNumber, build(operator))
    }
  }

  def buildOutputsVar(outputs: Seq[SubPlan.Output]): Var = {
    val builder = getStatic(Map.getClass.asType, "MODULE$", Map.getClass.asType)
      .invokeV("newBuilder", classOf[mutable.Builder[_, _]].asType)
    for {
      op <- outputs.map(_.getOperator).sortBy(_.getSerialNumber)
    } {
      builder.invokeI(NameTransformer.encode("+="),
        classOf[mutable.Builder[_, _]].asType,
        getStatic(Tuple2.getClass.asType, "MODULE$", Tuple2.getClass.asType).
          invokeV("apply", classOf[(_, _)].asType,
            context.branchKeys.getField(mb, op).asType(classOf[AnyRef].asType),
            vars(op.getOriginalSerialNumber).push().asType(classOf[AnyRef].asType))
          .asType(classOf[AnyRef].asType))
    }
    val map = builder.invokeI("result", classOf[AnyRef].asType).cast(classOf[Map[_, _]].asType)
    map.store(nextLocal.getAndAdd(map.size))
  }
}
