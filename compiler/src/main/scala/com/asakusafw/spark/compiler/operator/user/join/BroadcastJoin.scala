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
import com.asakusafw.spark.runtime.driver.{ BroadcastId, ShuffleKey }
import com.asakusafw.spark.runtime.fragment.Fragment
import com.asakusafw.spark.runtime.io.WritableSerDe
import com.asakusafw.spark.runtime.operator.DefaultMasterSelection
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._

trait BroadcastJoin
  extends JoinOperatorFragmentClassBuilder
  with ScalaIdioms {

  implicit def context: SparkClientCompiler.Context

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

  override def initFields(mb: MethodBuilder): Unit = {
    super.initFields(mb)

    import mb._ // scalastyle:ignore
    val broadcastsVar = `var`(classOf[Map[BroadcastId, Broadcast[_]]].asType, thisVar.nextLocal)

    val marker: MarkerOperator = {
      val opposites = masterInput.getOpposites
      assert(opposites.size == 1,
        s"The size of master inputs should be 1: ${opposites.size}")
      val opposite = opposites.head.getOwner
      assert(opposite.isInstanceOf[MarkerOperator],
        s"The master input should be marker operator: ${opposite}")
      assert(
        opposite.asInstanceOf[MarkerOperator].getAttribute(classOf[PlanMarker])
          == PlanMarker.BROADCAST,
        s"The master input should be BROADCAST marker operator: ${
          opposite.asInstanceOf[MarkerOperator].getAttribute(classOf[PlanMarker])
        }")
      opposite.asInstanceOf[MarkerOperator]
    }

    thisVar.push().putField(
      "masters",
      classOf[Map[_, _]].asType,
      applyMap(mb)(
        broadcastsVar.push(), context.broadcastIds.getField(mb, marker))
        .cast(classOf[Broadcast[_]].asType)
        .invokeV("value", classOf[AnyRef].asType)
        .cast(classOf[Map[_, _]].asType))
  }

  override def defAddMethod(mb: MethodBuilder, dataModelVar: Var): Unit = {
    import mb._ // scalastyle:ignore
    val keyVar = {
      val dataModelRef = context.jpContext.getDataModelLoader.load(txInput.getDataType)
      val group = txInput.getGroup

      val shuffleKey = pushNew(classOf[ShuffleKey].asType)
      shuffleKey.dup().invokeInit(
        if (group.getGrouping.isEmpty) {
          buildArray(mb, Type.BYTE_TYPE)(_ => ())
        } else {
          pushObject(mb)(WritableSerDe)
            .invokeV("serialize", classOf[Array[Byte]].asType,
              buildSeq(mb) { builder =>
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
        buildArray(mb, Type.BYTE_TYPE)(_ => ()))
      shuffleKey.store(dataModelVar.nextLocal)
    }

    val mVar = thisVar.push().getField("masters", classOf[Map[_, _]].asType)
      .invokeI("get", classOf[Option[_]].asType, keyVar.push().asType(classOf[AnyRef].asType))
      .invokeV("orNull", classOf[AnyRef].asType,
        pushObject(mb)(Predef)
          .invokeV("conforms", classOf[Predef.<:<[_, _]].asType))
      .cast(classOf[Seq[_]].asType)
      .store(keyVar.nextLocal)

    val mastersVar =
      mVar.push().ifNull({
        pushNew0(classOf[ArrayList[_]].asType).asType(classOf[JList[_]].asType)
      }, {
        pushObject(mb)(JavaConversions)
          .invokeV("seqAsJavaList", classOf[JList[_]].asType, mVar.push())
      }).store(mVar.nextLocal)

    val selectedVar = (masterSelection match {
      case Some((name, t)) =>
        getOperatorField(mb)
          .invokeV(
            name,
            t.getReturnType(),
            ({ () => mastersVar.push() } +:
              { () => dataModelVar.push() } +:
              operator.arguments.map { argument =>
                () => ldc(argument.value)(ClassTag(argument.resolveClass))
              }).zip(t.getArgumentTypes()).map {
                case (s, t) => s().asType(t)
              }: _*)
      case None =>
        pushObject(mb)(DefaultMasterSelection)
          .invokeV(
            "select",
            classOf[AnyRef].asType,
            mastersVar.push(),
            dataModelVar.push().asType(classOf[AnyRef].asType))
          .cast(masterType)
    }).store(mastersVar.nextLocal)

    join(mb, selectedVar, dataModelVar)

    `return`()
  }
}
