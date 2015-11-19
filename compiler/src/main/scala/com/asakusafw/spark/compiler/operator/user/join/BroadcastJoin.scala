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
package operator
package user
package join

import java.util.{ ArrayList, List => JList }

import scala.collection.JavaConversions
import scala.collection.JavaConversions._
import scala.reflect.ClassTag

import org.apache.spark.broadcast.Broadcast
import org.objectweb.asm.Type
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.lang.compiler.model.graph.{ MarkerOperator, OperatorInput, UserOperator }
import com.asakusafw.lang.compiler.planning.PlanMarker
import com.asakusafw.spark.compiler.spi.OperatorCompiler
import com.asakusafw.spark.runtime.driver.{ BroadcastId, ShuffleKey }
import com.asakusafw.spark.runtime.fragment.Fragment
import com.asakusafw.spark.runtime.io.WritableSerDe
import com.asakusafw.spark.runtime.operator.DefaultMasterSelection
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._
import com.asakusafw.spark.tools.asm4s._

trait BroadcastJoin
  extends JoinOperatorFragmentClassBuilder {

  implicit def context: OperatorCompiler.Context

  def masterInput: OperatorInput
  def txInput: OperatorInput

  def operator: UserOperator

  override def defFields(fieldDef: FieldDef): Unit = {
    super.defFields(fieldDef)

    fieldDef.newField("masters", classOf[Map[_, _]].asType,
      new TypeSignatureBuilder()
        .newClassType(classOf[Map[_, _]].asType) {
          _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[ShuffleKey].asType)
            .newTypeArgument(SignatureVisitor.INSTANCEOF) {
              _.newClassType(classOf[Seq[_]].asType) {
                _.newTypeArgument(SignatureVisitor.INSTANCEOF, masterType)
              }
            }
        }
        .build())
  }

  override def initFields()(implicit mb: MethodBuilder): Unit = {
    super.initFields()

    val thisVar :: broadcastsVar :: _ = mb.argVars

    val marker: Option[MarkerOperator] = {
      val opposites = masterInput.getOpposites
      assert(opposites.size <= 1,
        s"The size of master inputs should be 0 or 1: ${opposites.size} [${operator}]")
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

    thisVar.push().putField(
      "masters",
      classOf[Map[_, _]].asType,
      marker.map { marker =>
        applyMap(
          broadcastsVar.push(), context.broadcastIds.getField(marker))
          .cast(classOf[Broadcast[_]].asType)
          .invokeV("value", classOf[AnyRef].asType)
          .cast(classOf[Map[_, _]].asType)
      }.getOrElse {
        buildMap(_ => ())
      })
  }

  override def defAddMethod(dataModelVar: Var)(implicit mb: MethodBuilder): Unit = {
    val thisVar :: _ = mb.argVars

    val keyVar = {
      val dataModelRef = txInput.dataModelRef
      val group = txInput.getGroup

      val shuffleKey = pushNew(classOf[ShuffleKey].asType)
      shuffleKey.dup().invokeInit(
        if (group.getGrouping.isEmpty) {
          buildArray(Type.BYTE_TYPE)(_ => ())
        } else {
          pushObject(WritableSerDe)
            .invokeV("serialize", classOf[Array[Byte]].asType,
              buildSeq { builder =>
                for {
                  propertyName <- group.getGrouping
                  property = dataModelRef.findProperty(propertyName)
                } {
                  builder +=
                    dataModelVar.push().invokeV(
                      property.getDeclaration.getName, property.getType.asType)
                }
              })
        },
        buildArray(Type.BYTE_TYPE)(_ => ()))
      shuffleKey.store()
    }

    val mastersVar =
      thisVar.push().getField("masters", classOf[Map[ShuffleKey, Seq[_]]].asType)
        .invokeI("contains", Type.BOOLEAN_TYPE, keyVar.push().asType(classOf[AnyRef].asType))
        .ifTrue({
          pushObject(JavaConversions)
            .invokeV("seqAsJavaList", classOf[JList[_]].asType,
              applyMap(
                thisVar.push().getField("masters", classOf[Map[ShuffleKey, Seq[_]]].asType),
                keyVar.push())
                .cast(classOf[Seq[_]].asType))
        }, {
          pushNew0(classOf[ArrayList[_]].asType).asType(classOf[JList[_]].asType)
        })
        .store()

    val selectedVar = (masterSelection match {
      case Some((name, t)) =>
        getOperatorField()
          .invokeV(
            name,
            t.getReturnType(),
            ({ () => mastersVar.push() } +:
              { () => dataModelVar.push() } +:
              operator.arguments.map { argument =>
                () => ldc(argument.value)(ClassTag(argument.resolveClass), implicitly)
              }).zip(t.getArgumentTypes()).map {
                case (s, t) => s().asType(t)
              }: _*)
      case None =>
        pushObject(DefaultMasterSelection)
          .invokeV(
            "select",
            classOf[AnyRef].asType,
            mastersVar.push(),
            dataModelVar.push().asType(classOf[AnyRef].asType))
          .cast(masterType)
    }).store()

    join(selectedVar, dataModelVar)

    `return`()
  }
}
