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

import scala.collection.mutable
import scala.collection.JavaConversions._
import scala.reflect.NameTransformer

import org.objectweb.asm.{ Opcodes, Type }
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.lang.compiler.api.reference.DataModelReference
import com.asakusafw.lang.compiler.model.PropertyName
import com.asakusafw.lang.compiler.model.graph.{ Group, MarkerOperator }
import com.asakusafw.lang.compiler.planning.SubPlan
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.spark.compiler.planning.{ BroadcastInfo, SubPlanOutputInfo }
import com.asakusafw.spark.runtime.driver.ShuffleKey
import com.asakusafw.spark.runtime.io.WritableSerDe
import com.asakusafw.spark.runtime.rdd.BranchKey
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._

trait PreparingKey extends ClassBuilder {

  def context: SparkClientCompiler.Context

  def subplanOutputs: Seq[SubPlan.Output]

  override def defMethods(methodDef: MethodDef): Unit = {
    super.defMethods(methodDef)

    methodDef.newMethod("shuffleKey", classOf[ShuffleKey].asType,
      Seq(classOf[BranchKey].asType, classOf[AnyRef].asType)) { mb =>
        import mb._ // scalastyle:ignore
        val branchVar = `var`(classOf[BranchKey].asType, thisVar.nextLocal)
        val valueVar = `var`(classOf[AnyRef].asType, branchVar.nextLocal)

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
          val dataModelRef = context.jpContext.getDataModelLoader.load(op.getInput.getDataType)
          val dataModelType = dataModelRef.getDeclaration.asType

          val methodName = s"shuffleKey${i}"
          methodDef.newMethod(
            methodName,
            classOf[ShuffleKey].asType,
            Seq(dataModelType)) { mb =>
              defShuffleKey(mb, dataModelRef, partitionInfo)
            }

          branchVar.push().unlessNotEqual(context.branchKeys.getField(mb, op)) {
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
    mb: MethodBuilder,
    dataModelRef: DataModelReference,
    partitionInfo: Group): Unit = {
    import mb._ // scalastyle:ignore
    val dataModelVar = `var`(dataModelRef.getDeclaration.asType, thisVar.nextLocal)

    def buildSeq(propertyNames: Seq[PropertyName]): Stack = {
      val builder = getStatic(Seq.getClass.asType, "MODULE$", Seq.getClass.asType)
        .invokeV("newBuilder", classOf[mutable.Builder[_, _]].asType)

      propertyNames.foreach { propertyName =>
        val property = dataModelRef.findProperty(propertyName)

        builder.invokeI(
          NameTransformer.encode("+="),
          classOf[mutable.Builder[_, _]].asType,
          dataModelVar.push().invokeV(
            property.getDeclaration.getName, property.getType.asType)
            .asType(classOf[AnyRef].asType))
      }

      builder.invokeI("result", classOf[AnyRef].asType).cast(classOf[Seq[_]].asType)
    }

    val shuffleKey = pushNew(classOf[ShuffleKey].asType)
    shuffleKey.dup().invokeInit(
      if (partitionInfo.getGrouping.isEmpty) {
        getStatic(Array.getClass.asType, "MODULE$", Array.getClass.asType)
          .invokeV("emptyByteArray", classOf[Array[Byte]].asType)
      } else {
        getStatic(WritableSerDe.getClass.asType, "MODULE$", WritableSerDe.getClass.asType)
          .invokeV("serialize", classOf[Array[Byte]].asType, {
            buildSeq(partitionInfo.getGrouping)
          })
      },
      if (partitionInfo.getOrdering.isEmpty) {
        getStatic(Array.getClass.asType, "MODULE$", Array.getClass.asType)
          .invokeV("emptyByteArray", classOf[Array[Byte]].asType)
      } else {
        getStatic(WritableSerDe.getClass.asType, "MODULE$", WritableSerDe.getClass.asType)
          .invokeV("serialize", classOf[Array[Byte]].asType, {
            buildSeq(partitionInfo.getOrdering.map(_.getPropertyName))
          })
      })
    `return`(shuffleKey)
  }
}
