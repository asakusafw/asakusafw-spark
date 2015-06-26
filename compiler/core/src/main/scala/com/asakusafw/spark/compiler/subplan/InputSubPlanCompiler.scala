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

import java.lang.{ Boolean => JBoolean }
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.reflect.{ ClassTag, NameTransformer }

import org.apache.hadoop.io.NullWritable
import org.apache.spark.{ HashPartitioner, Partitioner }
import org.apache.spark.broadcast.Broadcast
import org.objectweb.asm.Type
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.lang.compiler.hadoop.InputFormatInfoExtension
import com.asakusafw.lang.compiler.model.graph._
import com.asakusafw.lang.compiler.planning.{ PlanMarker, SubPlan }
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.runtime.stage.input.TemporaryInputFormat
import com.asakusafw.spark.compiler.operator._
import com.asakusafw.spark.compiler.planning.SubPlanInfo
import com.asakusafw.spark.compiler.spi.{ OperatorCompiler, OperatorType, SubPlanCompiler }
import com.asakusafw.spark.runtime.fragment._
import com.asakusafw.spark.runtime.driver.BroadcastId
import com.asakusafw.spark.runtime.rdd.BranchKey
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._

class InputSubPlanCompiler extends SubPlanCompiler {

  def of: SubPlanInfo.DriverType = SubPlanInfo.DriverType.INPUT

  override def instantiator: Instantiator = InputSubPlanCompiler.InputDriverInstantiator

  override def compile(subplan: SubPlan)(implicit context: Context): Type = {
    val subPlanInfo = subplan.getAttribute(classOf[SubPlanInfo])
    val primaryOperator = subPlanInfo.getPrimaryOperator
    assert(primaryOperator.isInstanceOf[ExternalInput],
      s"The dominant operator should be external input: ${primaryOperator}")
    val operator = primaryOperator.asInstanceOf[ExternalInput]

    val (keyType, valueType, inputFormatType, paths, extraConfigurations) =
      (if (JBoolean.parseBoolean(
        context.jpContext.getOptions.get(SparkClientCompiler.Options.SparkInputDirect, true.toString)) == true) {
        Option(InputFormatInfoExtension.resolve(context.jpContext, operator.getName(), operator.getInfo()))
      } else None) match {
        case Some(info) =>
          (info.getKeyClass.asType, info.getValueClass.asType, info.getFormatClass.asType,
            None, Some(info.getExtraConfiguration.toMap))
        case None =>
          val inputRef = context.externalInputs.getOrElseUpdate(
            operator.getName,
            context.jpContext.addExternalInput(operator.getName, operator.getInfo))
          (classOf[NullWritable].asType,
            operator.getDataType.asType,
            classOf[TemporaryInputFormat[_]].asType,
            Some(inputRef.getPaths.toSeq.sorted),
            None)
      }

    val builder = new InputDriverClassBuilder(
      context.flowId,
      keyType,
      valueType,
      inputFormatType) {

      override val jpContext = context.jpContext

      override val branchKeys: BranchKeys = context.branchKeys

      override val label: String = subPlanInfo.getLabel

      override val subplanOutputs: Seq[SubPlan.Output] = subplan.getOutputs.toSeq

      override def defMethods(methodDef: MethodDef): Unit = {
        super.defMethods(methodDef)

        methodDef.newMethod("paths", classOf[Option[Set[String]]].asType, Seq.empty,
          new MethodSignatureBuilder()
            .newReturnType {
              _.newClassType(classOf[Option[_]].asType) {
                _.newTypeArgument(SignatureVisitor.INSTANCEOF) {
                  _.newClassType(classOf[Set[_]].asType) {
                    _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[String].asType)
                  }
                }
              }
            }
            .build()) { mb =>
            import mb._ // scalastyle:ignore
            `return`(
              paths match {
                case Some(paths) =>
                  getStatic(Option.getClass.asType, "MODULE$", Option.getClass.asType)
                    .invokeV("apply", classOf[Option[_]].asType, {
                      val builder = getStatic(Set.getClass.asType, "MODULE$", Set.getClass.asType)
                        .invokeV("newBuilder", classOf[mutable.Builder[_, _]].asType)
                      paths.foreach { path =>
                        builder.invokeI(NameTransformer.encode("+="),
                          classOf[mutable.Builder[_, _]].asType, ldc(path).asType(classOf[AnyRef].asType))
                      }
                      builder.invokeI("result", classOf[AnyRef].asType)
                    })
                case None =>
                  getStatic(None.getClass.asType, "MODULE$", None.getClass.asType)
              })
          }

        methodDef.newMethod("extraConfigurations", classOf[Map[String, String]].asType, Seq.empty,
          new MethodSignatureBuilder()
            .newReturnType {
              _.newClassType(classOf[Map[_, _]].asType) {
                _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[String].asType)
                  .newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[String].asType)
              }
            }
            .build()) { mb =>
            import mb._ // scalastyle:ignore
            `return`(
              extraConfigurations match {
                case Some(confs) =>
                  val builder = getStatic(Map.getClass.asType, "MODULE$", Map.getClass.asType)
                    .invokeV("newBuilder", classOf[mutable.Builder[_, _]].asType)

                  confs.foreach {
                    case (k, v) =>
                      builder.invokeI(NameTransformer.encode("+="),
                        classOf[mutable.Builder[_, _]].asType,
                        getStatic(Tuple2.getClass.asType, "MODULE$", Tuple2.getClass.asType)
                          .invokeV("apply", classOf[(String, String)].asType,
                            ldc(k).asType(classOf[AnyRef].asType),
                            ldc(v).asType(classOf[AnyRef].asType))
                          .asType(classOf[AnyRef].asType))
                  }

                  builder.invokeI("result", classOf[AnyRef].asType)
                    .cast(classOf[Map[String, String]].asType)
                case None =>
                  getStatic(Map.getClass.asType, "MODULE$", Map.getClass.asType)
                    .invokeV("empty", classOf[Map[String, String]].asType)
              })
          }

        methodDef.newMethod("fragments", classOf[(_, _)].asType, Seq(classOf[Map[BroadcastId, Broadcast[_]]].asType),
          new MethodSignatureBuilder()
            .newParameterType {
              _.newClassType(classOf[Map[_, _]].asType) {
                _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[BroadcastId].asType)
                  .newTypeArgument(SignatureVisitor.INSTANCEOF) {
                    _.newClassType(classOf[Broadcast[_]].asType) {
                      _.newTypeArgument()
                    }
                  }
              }
            }
            .newReturnType {
              _.newClassType(classOf[(_, _)].asType) {
                _.newTypeArgument(SignatureVisitor.INSTANCEOF) {
                  _.newClassType(classOf[Fragment[_]].asType) {
                    _.newTypeArgument(SignatureVisitor.INSTANCEOF, valueType)
                  }
                }
                  .newTypeArgument(SignatureVisitor.INSTANCEOF) {
                    _.newClassType(classOf[Map[_, _]].asType) {
                      _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[BranchKey].asType)
                        .newTypeArgument(SignatureVisitor.INSTANCEOF) {
                          _.newClassType(classOf[OutputFragment[_]].asType) {
                            _.newTypeArgument()
                          }
                        }
                    }
                  }
              }
            }
            .build()) { mb =>
            import mb._ // scalastyle:ignore
            val broadcastsVar = `var`(classOf[Map[BroadcastId, Broadcast[_]]].asType, thisVar.nextLocal)
            val nextLocal = new AtomicInteger(broadcastsVar.nextLocal)

            val fragmentBuilder = new FragmentTreeBuilder(mb, broadcastsVar, nextLocal)(
              OperatorCompiler.Context(
                flowId = context.flowId,
                jpContext = context.jpContext,
                branchKeys = context.branchKeys,
                broadcastIds = context.broadcastIds))
            val fragmentVar = fragmentBuilder.build(operator.getOperatorPort)
            val outputsVar = fragmentBuilder.buildOutputsVar(subplanOutputs)

            `return`(
              getStatic(Tuple2.getClass.asType, "MODULE$", Tuple2.getClass.asType).
                invokeV("apply", classOf[(_, _)].asType,
                  fragmentVar.push().asType(classOf[AnyRef].asType), outputsVar.push().asType(classOf[AnyRef].asType)))
          }
      }
    }

    context.jpContext.addClass(builder)
  }
}

object InputSubPlanCompiler {

  object InputDriverInstantiator extends Instantiator {

    override def newInstance(
      driverType: Type,
      subplan: SubPlan)(implicit context: Context): Var = {
      import context.mb._ // scalastyle:ignore

      val inputDriver = pushNew(driverType)
      inputDriver.dup().invokeInit(
        context.scVar.push(),
        context.hadoopConfVar.push(),
        context.broadcastsVar.push())
      inputDriver.store(context.nextLocal.getAndAdd(inputDriver.size))
    }
  }
}
