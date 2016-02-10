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
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.spark.compiler.spi.OperatorCompiler
import com.asakusafw.spark.runtime.fragment.Fragment
import com.asakusafw.spark.runtime.graph.BroadcastId
import com.asakusafw.spark.runtime.io.WritableSerDe
import com.asakusafw.spark.runtime.operator.DefaultMasterSelection
import com.asakusafw.spark.runtime.rdd.ShuffleKey
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._
import com.asakusafw.spark.tools.asm4s._

trait BroadcastJoin
  extends JoinOperatorFragmentClassBuilder {

  implicit def context: OperatorCompiler.Context

  protected def masters()(implicit mb: MethodBuilder): Stack = {
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
    marker.map { marker =>
      applyMap(
        broadcastsVar.push(), context.broadcastIds.getField(marker))
        .cast(classOf[Broadcast[_]].asType)
        .invokeV("value", classOf[AnyRef].asType)
        .cast(classOf[Map[_, _]].asType)
    }.getOrElse {
      buildMap(_ => ())
    }
  }

  override def defMethods(methodDef: MethodDef): Unit = {
    super.defMethods(methodDef)

    methodDef.newMethod(
      "key",
      classOf[AnyRef].asType,
      Seq(classOf[DataModel[_]].asType)) { implicit mb =>
        val thisVar :: txVar :: _ = mb.argVars
        `return`(
          thisVar.push().invokeV("key", classOf[ShuffleKey].asType, txVar.push().cast(txType)))
      }

    methodDef.newMethod(
      "key",
      classOf[ShuffleKey].asType,
      Seq(txType)) { implicit mb =>
        val thisVar :: txVar :: _ = mb.argVars

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
                      txVar.push().invokeV(
                        property.getDeclaration.getName, property.getType.asType)
                  }
                })
          },
          buildArray(Type.BYTE_TYPE)(_ => ()))
        `return`(shuffleKey)
      }
  }
}
