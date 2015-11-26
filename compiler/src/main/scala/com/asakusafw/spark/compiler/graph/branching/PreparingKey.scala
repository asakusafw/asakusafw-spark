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
package graph
package branching

import scala.collection.JavaConversions._

import org.objectweb.asm.Type

import com.asakusafw.lang.compiler.api.reference.DataModelReference
import com.asakusafw.lang.compiler.model.graph.Group
import com.asakusafw.lang.compiler.planning.SubPlan
import com.asakusafw.spark.compiler.planning.{ BroadcastInfo, SubPlanOutputInfo }
import com.asakusafw.spark.runtime.io.WritableSerDe
import com.asakusafw.spark.runtime.rdd.{ BranchKey, ShuffleKey }
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._
import com.asakusafw.spark.tools.asm4s._

trait PreparingKey extends ClassBuilder {

  implicit def context: PreparingKey.Context

  def subplanOutputs: Seq[SubPlan.Output]

  override def defMethods(methodDef: MethodDef): Unit = {
    super.defMethods(methodDef)

    methodDef.newMethod("shuffleKey", classOf[ShuffleKey].asType,
      Seq(classOf[BranchKey].asType, classOf[AnyRef].asType)) { implicit mb =>
        val thisVar :: branchVar :: valueVar :: _ = mb.argVars

        for {
          (output, i) <- subplanOutputs.sortBy(_.getOperator.getSerialNumber).zipWithIndex
          outputInfo <- Option(output.getAttribute(classOf[SubPlanOutputInfo]))
          partitionInfo <- outputInfo.getOutputType match {
            case SubPlanOutputInfo.OutputType.AGGREGATED |
              SubPlanOutputInfo.OutputType.PARTITIONED =>
              Option(outputInfo.getPartitionInfo)
            case SubPlanOutputInfo.OutputType.BROADCAST =>
              Option(output.getAttribute(classOf[BroadcastInfo])).map(_.getFormatInfo)
            case _ => None
          }
        } {
          val op = output.getOperator
          val dataModelRef = op.getInput.dataModelRef
          val dataModelType = dataModelRef.getDeclaration.asType

          val methodName = s"shuffleKey${i}"
          methodDef.newMethod(
            methodName,
            classOf[ShuffleKey].asType,
            Seq(dataModelType)) { implicit mb =>
              defShuffleKey(dataModelRef, partitionInfo)
            }

          branchVar.push().unlessNotEqual(context.branchKeys.getField(op)) {
            `return`(
              thisVar.push().invokeV(
                methodName,
                classOf[ShuffleKey].asType,
                valueVar.push().cast(dataModelType)))
          }
        }
        `return`(pushNull(classOf[ShuffleKey].asType))
      }
  }

  private[this] def defShuffleKey(
    dataModelRef: DataModelReference,
    partitionInfo: Group)(
      implicit mb: MethodBuilder): Unit = {
    val thisVar :: dataModelVar :: _ = mb.argVars

    val shuffleKey = pushNew(classOf[ShuffleKey].asType)
    shuffleKey.dup().invokeInit(
      if (partitionInfo.getGrouping.isEmpty) {
        buildArray(Type.BYTE_TYPE)(_ => ())
      } else {
        pushObject(WritableSerDe)
          .invokeV(
            "serialize",
            classOf[Array[Byte]].asType,
            buildSeq { builder =>
              for {
                propertyName <- partitionInfo.getGrouping
                property = dataModelRef.findProperty(propertyName)
              } {
                builder +=
                  dataModelVar.push().invokeV(
                    property.getDeclaration.getName, property.getType.asType)
              }
            })
      },
      if (partitionInfo.getOrdering.isEmpty) {
        buildArray(Type.BYTE_TYPE)(_ => ())
      } else {
        pushObject(WritableSerDe)
          .invokeV(
            "serialize",
            classOf[Array[Byte]].asType,
            buildSeq { builder =>
              for {
                propertyName <- partitionInfo.getOrdering.map(_.getPropertyName)
                property = dataModelRef.findProperty(propertyName)
              } {
                builder +=
                  dataModelVar.push().invokeV(
                    property.getDeclaration.getName, property.getType.asType)
              }
            })
      })
    `return`(shuffleKey)
  }
}

object PreparingKey {

  trait Context
    extends DataModelLoaderProvider {

    def branchKeys: BranchKeys
  }
}
