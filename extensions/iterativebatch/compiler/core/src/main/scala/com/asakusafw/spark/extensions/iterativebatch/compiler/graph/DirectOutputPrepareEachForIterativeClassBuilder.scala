/*
 * Copyright 2011-2017 Asakusa Framework Team.
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
package com.asakusafw.spark.extensions.iterativebatch.compiler
package graph

import java.util.concurrent.atomic.AtomicLong

import scala.collection.JavaConversions._
import scala.collection.mutable

import org.apache.spark.Partitioner
import org.objectweb.asm.{ Opcodes, Type }
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.lang.compiler.api.reference.DataModelReference
import com.asakusafw.lang.compiler.extension.directio.{ DirectFileOutputModel, OutputPattern }
import com.asakusafw.lang.compiler.model.graph.{ ExternalOutput, Group }
import com.asakusafw.runtime.directio.DataFormat
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.runtime.value.{ StringOption, ValueOption }
import com.asakusafw.runtime.stage.directio.StringTemplate.Format
import com.asakusafw.spark.compiler._
import com.asakusafw.spark.compiler.directio.OutputPatternGeneratorClassBuilder
import com.asakusafw.spark.compiler.graph.{ CacheStrategy, LabelField }
import com.asakusafw.spark.compiler.spi.NodeCompiler
import com.asakusafw.spark.compiler.util.SparkIdioms._
import com.asakusafw.spark.runtime.JobContext
import com.asakusafw.spark.runtime.directio.OutputPatternGenerator
import com.asakusafw.spark.runtime.directio.OutputPatternGenerator.Fragment
import com.asakusafw.spark.runtime.graph.{
  SortOrdering,
  Source
}
import com.asakusafw.spark.runtime.rdd.{ BranchKey, ShuffleKey }
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._
import com.asakusafw.spark.tools.asm4s._

import com.asakusafw.spark.extensions.iterativebatch.compiler.graph.DirectOutputPrepareEachForIterativeClassBuilder._ // scalastyle:ignore
import com.asakusafw.spark.extensions.iterativebatch.runtime.graph.{
  DirectOutputPrepareFlatEachForIterative,
  DirectOutputPrepareGroupEachForIterative
}

abstract class DirectOutputPrepareEachFlatForIterativeClassBuilder(
  operator: ExternalOutput)(
    model: DirectFileOutputModel)(
      val label: String)(
        implicit val context: NodeCompiler.Context)
  extends ClassBuilder(
    Type.getType(
      s"L${GeneratedClassPackageInternalName}/${context.flowId}/graph/DirectOutputPrepareFlatEachForIterative$$${nextId(flat = true)};"), // scalastyle:ignore
    new ClassSignatureBuilder()
      .newSuperclass {
        _.newClassType(classOf[DirectOutputPrepareFlatEachForIterative[_]].asType) {
          _.newTypeArgument(SignatureVisitor.INSTANCEOF, operator.getOperatorPort.dataModelType)
        }
      },
    classOf[DirectOutputPrepareFlatEachForIterative[_]].asType)
  with LabelField {
  self: CacheStrategy =>

  private val dataModelType = operator.getOperatorPort.dataModelType

  override def defConstructors(ctorDef: ConstructorDef): Unit = {
    ctorDef.newInit(Seq(
      classOf[Seq[(Source, BranchKey)]].asType,
      classOf[JobContext].asType),
      new MethodSignatureBuilder()
        .newParameterType {
          _.newClassType(classOf[Seq[_]].asType) {
            _.newTypeArgument(SignatureVisitor.INSTANCEOF) {
              _.newClassType(classOf[(_, _)].asType) {
                _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[Source].asType)
                  .newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[BranchKey].asType)
              }
            }
          }
        }
        .newParameterType(classOf[JobContext].asType)
        .newVoidReturnType()) { implicit mb =>

        val thisVar :: prevsVar :: jobContextVar :: _ = mb.argVars

        thisVar.push().invokeInit(
          superType,
          prevsVar.push(),
          classTag(dataModelType),
          jobContextVar.push())
        initMixIns()
      }
  }

  override def defMethods(methodDef: MethodDef): Unit = {
    super.defMethods(methodDef)

    methodDef.newMethod(
      "newDataModel", classOf[DataModel[_]].asType, Seq.empty) { implicit mb =>
        val thisVar :: _ = mb.argVars
        `return`(thisVar.push().invokeV("newDataModel", dataModelType))
      }

    methodDef.newMethod(
      "newDataModel", dataModelType, Seq.empty) { implicit mb =>
        `return`(pushNew0(dataModelType))
      }

    methodDef.newMethod("basePath", classOf[String].asType, Seq.empty) { implicit mb =>
      `return`(ldc(model.getBasePath))
    }

    methodDef.newMethod("resourcePattern", classOf[String].asType, Seq.empty) { implicit mb =>
      `return`(ldc(model.getResourcePattern))
    }
  }
}

abstract class DirectOutputPrepareGroupEachForIterativeClassBuilder(
  operator: ExternalOutput)(
    pattern: OutputPattern,
    model: DirectFileOutputModel)(
      val label: String)(
        implicit val context: NodeCompiler.Context)
  extends ClassBuilder(
    Type.getType(
      s"L${GeneratedClassPackageInternalName}/${context.flowId}/graph/DirectOutputPrepareGroupEachForIterative$$${nextId(flat = false)};"), // scalastyle:ignore
    new ClassSignatureBuilder()
      .newSuperclass {
        _.newClassType(classOf[DirectOutputPrepareGroupEachForIterative[_]].asType) {
          _.newTypeArgument(SignatureVisitor.INSTANCEOF, operator.getOperatorPort.dataModelType)
        }
      },
    classOf[DirectOutputPrepareGroupEachForIterative[_]].asType)
  with LabelField {
  self: CacheStrategy =>

  private val dataModelRef = operator.getOperatorPort.dataModelRef
  private val dataModelType = operator.getOperatorPort.dataModelType

  override def defFields(fieldDef: FieldDef): Unit = {
    super.defFields(fieldDef)

    fieldDef.newField(
      Opcodes.ACC_PRIVATE | Opcodes.ACC_TRANSIENT,
      "outputPatternGenerator",
      classOf[OutputPatternGenerator[_]].asType,
      new TypeSignatureBuilder()
        .newClassType(classOf[OutputPatternGenerator[_]].asType) {
          _.newTypeArgument(SignatureVisitor.INSTANCEOF, dataModelType)
        })

    fieldDef.newField(
      Opcodes.ACC_PRIVATE | Opcodes.ACC_TRANSIENT,
      "sortOrdering",
      classOf[SortOrdering].asType,
      new TypeSignatureBuilder()
        .newClassType(classOf[Ordering[_]].asType) {
          _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[ShuffleKey].asType)
        })
  }

  override def defConstructors(ctorDef: ConstructorDef): Unit = {
    ctorDef.newInit(Seq(
      classOf[Seq[(Source, BranchKey)]].asType,
      classOf[Partitioner].asType,
      classOf[JobContext].asType),
      new MethodSignatureBuilder()
        .newParameterType {
          _.newClassType(classOf[Seq[_]].asType) {
            _.newTypeArgument(SignatureVisitor.INSTANCEOF) {
              _.newClassType(classOf[(_, _)].asType) {
                _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[Source].asType)
                  .newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[BranchKey].asType)
              }
            }
          }
        }
        .newParameterType(classOf[Partitioner].asType)
        .newParameterType(classOf[JobContext].asType)
        .newVoidReturnType()) { implicit mb =>

        val thisVar :: prevsVar :: partVar :: jobContextVar :: _ = mb.argVars

        thisVar.push().invokeInit(
          superType,
          prevsVar.push(),
          partVar.push(),
          classTag(dataModelType),
          jobContextVar.push())
        initMixIns()
      }
  }

  override def defMethods(methodDef: MethodDef): Unit = {
    super.defMethods(methodDef)

    methodDef.newMethod(
      "newDataModel", classOf[DataModel[_]].asType, Seq.empty) { implicit mb =>
        val thisVar :: _ = mb.argVars
        `return`(thisVar.push().invokeV("newDataModel", dataModelType))
      }

    methodDef.newMethod(
      "newDataModel", dataModelType, Seq.empty) { implicit mb =>
        `return`(pushNew0(dataModelType))
      }

    methodDef.newMethod("basePath", classOf[String].asType, Seq.empty) { implicit mb =>
      `return`(ldc(model.getBasePath))
    }

    methodDef.newMethod(
      "outputPatternGenerator", classOf[OutputPatternGenerator[_]].asType, Seq.empty,
      new MethodSignatureBuilder()
        .newReturnType {
          _.newClassType(classOf[OutputPatternGenerator[_]].asType) {
            _.newTypeArgument(SignatureVisitor.INSTANCEOF, dataModelType)
          }
        }) { implicit mb =>

        val thisVar :: _ = mb.argVars

        thisVar.push().getField(
          "outputPatternGenerator", classOf[OutputPatternGenerator[_]].asType).unlessNotNull {
            thisVar.push().putField(
              "outputPatternGenerator", {
                val generator =
                  pushNew(OutputPatternGeneratorClassBuilder.getOrCompile(dataModelRef))
                generator.dup().invokeInit(
                  buildSeq { builder =>
                    var randoms = 0
                    pattern.getResourcePattern.foreach { segment =>
                      segment.getKind match {
                        case OutputPattern.SourceKind.NOTHING =>
                          builder +=
                            pushObject(OutputPatternGenerator)
                            .invokeV("constant", classOf[Fragment].asType,
                              ldc(segment.getArgument))

                        case OutputPattern.SourceKind.PROPERTY =>
                          segment.getFormat match {
                            case Format.NATURAL =>
                              builder +=
                                pushObject(OutputPatternGenerator)
                                .invokeV("natural", classOf[Fragment].asType,
                                  ldc(segment.getTarget.getName.toMemberName))
                            case Format.DATE =>
                              builder +=
                                pushObject(OutputPatternGenerator)
                                .invokeV("date", classOf[Fragment].asType,
                                  ldc(segment.getTarget.getName.toMemberName),
                                  ldc(segment.getArgument))
                            case Format.DATETIME =>
                              builder +=
                                pushObject(OutputPatternGenerator)
                                .invokeV("dateTime", classOf[Fragment].asType,
                                  ldc(segment.getTarget.getName.toMemberName),
                                  ldc(segment.getArgument))
                            case _ =>
                              throw new AssertionError(
                                s"Unknown StringTemplate.Format: ${segment.getFormat}")
                          }

                        case OutputPattern.SourceKind.RANDOM =>
                          builder +=
                            pushObject(OutputPatternGenerator)
                            .invokeV("random", classOf[Fragment].asType,
                              ldc(0xcafebabe + randoms * 31),
                              ldc(segment.getRandomNumber.getLowerBound),
                              ldc(segment.getRandomNumber.getUpperBound))
                          randoms += 1

                        case _ =>
                          throw new AssertionError(
                            s"Unknown OutputPattern.SourceKind: ${segment.getKind}")
                      }
                    }
                  })
                generator.asType(classOf[OutputPatternGenerator[_]].asType)
              })
          }
        `return`(
          thisVar.push().getField(
            "outputPatternGenerator", classOf[OutputPatternGenerator[_]].asType))
      }

    methodDef.newMethod(
      "sortOrdering", classOf[SortOrdering].asType, Seq.empty,
      new MethodSignatureBuilder()
        .newReturnType {
          _.newClassType(classOf[Ordering[_]].asType) {
            _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[ShuffleKey].asType)
          }
        }) { implicit mb =>

        val thisVar :: _ = mb.argVars

        thisVar.push().getField("sortOrdering", classOf[SortOrdering].asType).unlessNotNull {
          thisVar.push().putField(
            "sortOrdering",
            sortOrdering(
              Seq(classOf[StringOption].asType, classOf[StringOption].asType),
              dataModelRef.orderingTypes(
                pattern.getOrders.map { order =>
                  new Group.Ordering(
                    order.getTarget.getName,
                    if (order.isAscend) Group.Direction.ASCENDANT else Group.Direction.DESCENDANT)
                })))
        }
        `return`(thisVar.push().getField("sortOrdering", classOf[SortOrdering].asType))
      }

    methodDef.newMethod(
      "orderings", classOf[Seq[ValueOption[_]]].asType, Seq(classOf[DataModel[_]].asType),
      new MethodSignatureBuilder()
        .newParameterType {
          _.newClassType(classOf[DataModel[_]].asType) {
            _.newTypeArgument()
          }
        }
        .newReturnType {
          _.newClassType(classOf[Seq[_]].asType) {
            _.newTypeArgument(SignatureVisitor.INSTANCEOF) {
              _.newClassType(classOf[ValueOption[_]].asType) {
                _.newTypeArgument()
              }
            }
          }
        }) { implicit mb =>

        val thisVar :: dataModelVar :: _ = mb.argVars

        `return`(
          thisVar.push().invokeV("orderings", classOf[Seq[ValueOption[_]]].asType,
            dataModelVar.push().cast(dataModelType)))
      }

    methodDef.newMethod(
      "orderings", classOf[Seq[ValueOption[_]]].asType, Seq(dataModelType),
      new MethodSignatureBuilder()
        .newParameterType(dataModelType)
        .newReturnType {
          _.newClassType(classOf[Seq[_]].asType) {
            _.newTypeArgument(SignatureVisitor.INSTANCEOF) {
              _.newClassType(classOf[ValueOption[_]].asType) {
                _.newTypeArgument()
              }
            }
          }
        }) { implicit mb =>

        val thisVar :: dataModelVar :: _ = mb.argVars

        `return`(
          buildSeq { builder =>
            pattern.getOrders.foreach { order =>
              val property = dataModelRef.findProperty(order.getTarget.getName)
              builder +=
                dataModelVar.push().invokeV(
                  property.getDeclaration.getName, property.getType.asType)
            }
          })
      }
  }
}

object DirectOutputPrepareEachForIterativeClassBuilder {

  private[this] val curIds: mutable.Map[NodeCompiler.Context, (AtomicLong, AtomicLong)] =
    mutable.WeakHashMap.empty

  def nextId(flat: Boolean)(implicit context: NodeCompiler.Context): Long = {
    val ids = curIds.getOrElseUpdate(context, (new AtomicLong(0L), new AtomicLong(0)))
    (if (flat) ids._1 else ids._2).getAndIncrement()
  }
}
