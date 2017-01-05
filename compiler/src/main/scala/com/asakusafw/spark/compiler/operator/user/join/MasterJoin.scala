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
package com.asakusafw.spark.compiler
package operator.user.join

import scala.collection.JavaConversions._
import scala.reflect.ClassTag

import org.objectweb.asm.Type

import com.asakusafw.lang.compiler.analyzer.util.{ JoinedModelUtil, PropertyMapping }
import com.asakusafw.lang.compiler.model.graph.UserOperator
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.spark.compiler.spi.OperatorCompiler
import com.asakusafw.spark.runtime.util.ValueOptionOps
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._
import com.asakusafw.spark.tools.asm4s._
import com.asakusafw.vocabulary.operator.{ MasterJoin => MasterJoinOp }

trait MasterJoin extends JoinOperatorFragmentClassBuilder {

  implicit def context: OperatorCompiler.Context

  val joinedType: Type = operator.outputs(MasterJoinOp.ID_OUTPUT_JOINED).dataModelType

  val mappings = JoinedModelUtil.getPropertyMappings(context.classLoader, operator).toSeq

  override def defMethods(methodDef: MethodDef): Unit = {
    super.defMethods(methodDef)

    methodDef.newMethod(
      "join",
      Seq(
        classOf[DataModel[_]].asType,
        classOf[DataModel[_]].asType,
        classOf[DataModel[_]].asType),
      new MethodSignatureBuilder()
        .newParameterType {
          _.newClassType(classOf[DataModel[_]].asType) {
            _.newTypeArgument()
          }
        }
        .newParameterType {
          _.newClassType(classOf[DataModel[_]].asType) {
            _.newTypeArgument()
          }
        }
        .newParameterType {
          _.newClassType(classOf[DataModel[_]].asType) {
            _.newTypeArgument()
          }
        }
        .newVoidReturnType()) { implicit mb =>
        val thisVar :: masterVar :: txVar :: joinedVar :: _ = mb.argVars
        thisVar.push().invokeV(
          "join",
          masterVar.push().cast(masterType),
          txVar.push().cast(txType),
          joinedVar.push().cast(joinedType))
        `return`()
      }

    methodDef.newMethod(
      "join",
      Seq(masterType, txType, joinedType)) { implicit mb =>
        val thisVar :: masterVar :: txVar :: joinedVar :: _ = mb.argVars

        val vars = Seq(masterVar, txVar)

        mappings.foreach { mapping =>
          val src = operator.inputs.indexOf(mapping.getSourcePort)
          val srcVar = vars(src)
          val srcProperty =
            operator.inputs(src).dataModelRef.findProperty(mapping.getSourceProperty)
          assert(mapping.getDestinationPort == operator.outputs(MasterJoinOp.ID_OUTPUT_JOINED),
            "The destination port should be the same as the port for "
              + "MasterJoinOp.ID_OUTPUT_JOINED: "
              + s"(${mapping.getDestinationPort}, "
              + s"${operator.outputs(MasterJoinOp.ID_OUTPUT_JOINED)}) [${operator}]")
          val destProperty =
            operator.outputs(MasterJoinOp.ID_OUTPUT_JOINED).dataModelRef
              .findProperty(mapping.getDestinationProperty)

          assert(srcProperty.getType.asType == destProperty.getType.asType,
            "The source and destination types should be the same: "
              + s"(${srcProperty.getType}, ${destProperty.getType}) [${operator}]")

          pushObject(ValueOptionOps)
            .invokeV(
              "copy",
              srcVar.push()
                .invokeV(srcProperty.getDeclaration.getName, srcProperty.getType.asType),
              joinedVar.push()
                .invokeV(destProperty.getDeclaration.getName, destProperty.getType.asType))
        }

        `return`()
      }
  }
}
