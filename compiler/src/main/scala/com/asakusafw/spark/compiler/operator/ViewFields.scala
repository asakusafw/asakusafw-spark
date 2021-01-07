/*
 * Copyright 2011-2021 Asakusa Framework Team.
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

import org.apache.spark.broadcast.{ Broadcast => Broadcasted }
import org.objectweb.asm.Opcodes

import com.asakusafw.lang.compiler.model.graph.{ MarkerOperator, OperatorInput }
import com.asakusafw.lang.compiler.planning.PlanMarker
import com.asakusafw.runtime.core.GroupView
import com.asakusafw.spark.compiler.graph.BroadcastIds
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._
import com.asakusafw.spark.tools.asm4s._

trait ViewFields extends ClassBuilder {

  implicit def context: ViewFields.Context

  def operatorInputs: Seq[OperatorInput]

  lazy val viewInputs: Seq[OperatorInput] =
    operatorInputs.filter(_.getInputUnit == OperatorInput.InputUnit.WHOLE)

  override def defFields(fieldDef: FieldDef): Unit = {
    super.defFields(fieldDef)

    viewInputs.zipWithIndex.foreach {
      case (input, i) =>
        fieldDef.newField(
          Opcodes.ACC_PRIVATE | Opcodes.ACC_FINAL,
          s"view${i}",
          classOf[GroupView[_]].asType)
    }
  }

  def initViewFields(broadcastsVar: Var)(implicit mb: MethodBuilder): Unit = {
    val thisVar :: _ = mb.argVars

    viewInputs.zipWithIndex.foreach {
      case (input, i) =>
        val marker: Option[MarkerOperator] = {
          val opposites = input.getOpposites
          assert(opposites.size <= 1,
            s"The size of broadcast inputs should be 0 or 1: ${opposites.size}")
          opposites.headOption.map { opposite =>
            val operator = opposite.getOwner
            assert(operator.isInstanceOf[MarkerOperator],
              s"The master input should be marker operator: ${operator} [${operator}]")
            assert(
              operator.asInstanceOf[MarkerOperator].getAttribute(classOf[PlanMarker])
                == PlanMarker.BROADCAST,
              s"The master input should be BROADCAST marker operator: ${
                operator.asInstanceOf[MarkerOperator].getAttribute(classOf[PlanMarker])
              } [${operator}]")
            operator.asInstanceOf[MarkerOperator]
          }
        }

        val keyElementTypes = input.dataModelRef.groupingTypes(input.getGroup.getGrouping)

        val mapGroupViewType = MapGroupViewClassBuilder.getOrCompile(keyElementTypes)

        thisVar.push().putField(
          s"view${i}", {
            val mapGroupView = pushNew(mapGroupViewType)
            mapGroupView.dup().invokeInit(
              marker.map { marker =>
                applyMap(
                  broadcastsVar.push(), context.broadcastIds.getField(marker))
                  .cast(classOf[Broadcasted[_]].asType)
                  .invokeV("value", classOf[AnyRef].asType)
                  .cast(classOf[Map[_, _]].asType)
              }.getOrElse {
                buildMap(_ => ())
              })
            mapGroupView.asType(classOf[GroupView[_]].asType)
          })
    }
  }

  def getViewField(input: OperatorInput)(implicit mb: MethodBuilder): Stack = {
    val thisVar :: _ = mb.argVars
    val i = viewInputs.indexOf(input)
    assert(i >= 0,
      s"The input unit of ${input} is not InputUnit.WHOLE: ${input.getInputUnit}")
    thisVar.push().getField(s"view${i}", classOf[GroupView[_]].asType)
  }
}

object ViewFields {

  trait Context
    extends CompilerContext
    with DataModelLoaderProvider {

    def broadcastIds: BroadcastIds
  }
}
