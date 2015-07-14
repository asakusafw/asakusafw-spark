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

import scala.collection.JavaConversions._

import org.apache.spark.Partitioner
import org.objectweb.asm.{ Opcodes, Type }
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.lang.compiler.model.graph.Group
import com.asakusafw.lang.compiler.planning.SubPlan
import com.asakusafw.spark.compiler.ordering.SortOrderingClassBuilder
import com.asakusafw.spark.compiler.planning.{ BroadcastInfo, SubPlanOutputInfo }
import com.asakusafw.spark.runtime.driver.ShuffleKey
import com.asakusafw.spark.runtime.rdd.BranchKey
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._

trait OrderingsField
  extends ClassBuilder
  with ScalaIdioms {

  implicit def context: SparkClientCompiler.Context

  def subplanOutputs: Seq[SubPlan.Output]

  override def defFields(fieldDef: FieldDef): Unit = {
    super.defFields(fieldDef)

    fieldDef.newField(
      Opcodes.ACC_PRIVATE | Opcodes.ACC_TRANSIENT,
      "orderings",
      classOf[Map[_, _]].asType,
      new TypeSignatureBuilder()
        .newClassType(classOf[Map[_, _]].asType) {
          _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[BranchKey].asType)
            .newTypeArgument(SignatureVisitor.INSTANCEOF) {
              _.newClassType(classOf[Ordering[_]].asType) {
                _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[ShuffleKey].asType)
              }
            }
        }
        .build())
  }

  override def defMethods(methodDef: MethodDef): Unit = {
    super.defMethods(methodDef)

    methodDef.newMethod("orderings", classOf[Map[_, _]].asType, Seq.empty,
      new MethodSignatureBuilder()
        .newReturnType {
          _.newClassType(classOf[Map[_, _]].asType) {
            _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[BranchKey].asType)
              .newTypeArgument(SignatureVisitor.INSTANCEOF) {
                _.newClassType(classOf[Ordering[_]].asType) {
                  _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[ShuffleKey].asType)
                }
              }
          }
        }
        .build()) { mb =>
        import mb._ // scalastyle:ignore
        thisVar.push().getField("orderings", classOf[Map[_, _]].asType).unlessNotNull {
          thisVar.push().putField("orderings", classOf[Map[_, _]].asType, initOrderings(mb))
        }
        `return`(thisVar.push().getField("orderings", classOf[Map[_, _]].asType))
      }
  }

  def getOrderingsField(mb: MethodBuilder): Stack = {
    import mb._ // scalastyle:ignore
    thisVar.push().invokeV("orderings", classOf[Map[_, _]].asType)
  }

  private def initOrderings(mb: MethodBuilder): Stack = {
    import mb._ // scalastyle:ignore
    buildMap(mb) { builder =>
      for {
        output <- subplanOutputs.sortBy(_.getOperator.getSerialNumber)
        outputInfo <- Option(output.getAttribute(classOf[SubPlanOutputInfo]))
        partitionInfo <- outputInfo.getOutputType match {
          case SubPlanOutputInfo.OutputType.AGGREGATED | SubPlanOutputInfo.OutputType.PARTITIONED =>
            Option(outputInfo.getPartitionInfo)
          case SubPlanOutputInfo.OutputType.BROADCAST =>
            Option(output.getAttribute(classOf[BroadcastInfo])).map(_.getFormatInfo)
          case _ => None
        }
      } {
        val dataModelRef =
          context.jpContext.getDataModelLoader.load(output.getOperator.getInput.getDataType)
        builder += (
          context.branchKeys.getField(mb, output.getOperator),
          pushNew0(
            SortOrderingClassBuilder.getOrCompile(
              partitionInfo.getGrouping.map { grouping =>
                dataModelRef.findProperty(grouping).getType.asType
              },
              partitionInfo.getOrdering.map { ordering =>
                (dataModelRef.findProperty(ordering.getPropertyName).getType.asType,
                  ordering.getDirection == Group.Direction.ASCENDANT)
              })))
      }
    }
  }
}
