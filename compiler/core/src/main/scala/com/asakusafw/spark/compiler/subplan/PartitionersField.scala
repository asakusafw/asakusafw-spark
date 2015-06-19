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

import org.apache.spark.{ HashPartitioner, Partitioner, SparkConf, SparkContext }
import org.objectweb.asm.{ Opcodes, Type }
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.lang.compiler.api.JobflowProcessor.{ Context => JPContext }
import com.asakusafw.lang.compiler.planning.{ PlanMarker, SubPlan }
import com.asakusafw.spark.compiler.planning.SubPlanOutputInfo
import com.asakusafw.spark.runtime.rdd.{ BranchKey, IdentityPartitioner }
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._

trait PartitionersField extends ClassBuilder with NumPartitions {

  def flowId: String

  def jpContext: JPContext

  def branchKeys: BranchKeys

  def subplanOutputs: Seq[SubPlan.Output]

  override def defFields(fieldDef: FieldDef): Unit = {
    super.defFields(fieldDef)

    fieldDef.newField(
      Opcodes.ACC_PRIVATE | Opcodes.ACC_TRANSIENT,
      "partitioners",
      classOf[Map[_, _]].asType,
      new TypeSignatureBuilder()
        .newClassType(classOf[Map[_, _]].asType) {
          _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[BranchKey].asType)
            .newTypeArgument(SignatureVisitor.INSTANCEOF) {
              _.newClassType(classOf[Option[_]].asType) {
                _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[Partitioner].asType)
              }
            }
        }
        .build())
  }

  override def defMethods(methodDef: MethodDef): Unit = {
    super.defMethods(methodDef)

    methodDef.newMethod("partitioners", classOf[Map[_, _]].asType, Seq.empty,
      new MethodSignatureBuilder()
        .newReturnType {
          _.newClassType(classOf[Map[_, _]].asType) {
            _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[BranchKey].asType)
              .newTypeArgument(SignatureVisitor.INSTANCEOF) {
                _.newClassType(classOf[Option[_]].asType) {
                  _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[Partitioner].asType)
                }
              }
          }
        }
        build ()) { mb =>
        import mb._
        thisVar.push().getField("partitioners", classOf[Map[_, _]].asType).unlessNotNull {
          thisVar.push().putField("partitioners", classOf[Map[_, _]].asType, initPartitioners(mb))
        }
        `return`(thisVar.push().getField("partitioners", classOf[Map[_, _]].asType))
      }
  }

  def getPartitionersField(mb: MethodBuilder): Stack = {
    import mb._
    thisVar.push().invokeV("partitioners", classOf[Map[_, _]].asType)
  }

  private def initPartitioners(mb: MethodBuilder): Stack = {
    import mb._
    val builder = getStatic(Map.getClass.asType, "MODULE$", Map.getClass.asType)
      .invokeV("newBuilder", classOf[mutable.Builder[_, _]].asType)
    for {
      output <- subplanOutputs.sortBy(_.getOperator.getSerialNumber)
      outputInfo <- Option(output.getAttribute(classOf[SubPlanOutputInfo]))
      if outputInfo.getOutputType == SubPlanOutputInfo.OutputType.DONT_CARE ||
        outputInfo.getOutputType == SubPlanOutputInfo.OutputType.AGGREGATED ||
        outputInfo.getOutputType == SubPlanOutputInfo.OutputType.PARTITIONED ||
        outputInfo.getOutputType == SubPlanOutputInfo.OutputType.BROADCAST
    } {
      builder.invokeI(
        NameTransformer.encode("+="),
        classOf[mutable.Builder[_, _]].asType,
        getStatic(Tuple2.getClass.asType, "MODULE$", Tuple2.getClass.asType).
          invokeV("apply", classOf[(_, _)].asType,
            branchKeys.getField(mb, output.getOperator).asType(classOf[AnyRef].asType), {
              outputInfo.getOutputType match {
                case SubPlanOutputInfo.OutputType.DONT_CARE =>
                  getStatic(None.getClass.asType, "MODULE$", None.getClass.asType)
                case SubPlanOutputInfo.OutputType.AGGREGATED |
                  SubPlanOutputInfo.OutputType.PARTITIONED if outputInfo.getPartitionInfo.getGrouping.nonEmpty =>
                  getStatic(Option.getClass.asType, "MODULE$", Option.getClass.asType)
                    .invokeV("apply", classOf[Option[_]].asType, {
                      val partitioner = pushNew(classOf[HashPartitioner].asType)
                      partitioner.dup().invokeInit(
                        numPartitions(mb, thisVar.push().invokeV("sc", classOf[SparkContext].asType))(output))
                      partitioner
                    }.asType(classOf[AnyRef].asType))
                case _ =>
                  getStatic(Option.getClass.asType, "MODULE$", Option.getClass.asType)
                    .invokeV("apply", classOf[Option[_]].asType, {
                      val partitioner = pushNew(classOf[HashPartitioner].asType)
                      partitioner.dup().invokeInit(ldc(1))
                      partitioner
                    }.asType(classOf[AnyRef].asType))
              }
            }.asType(classOf[AnyRef].asType))
          .asType(classOf[AnyRef].asType))
    }
    builder.invokeI("result", classOf[AnyRef].asType).cast(classOf[Map[_, _]].asType)
  }
}
