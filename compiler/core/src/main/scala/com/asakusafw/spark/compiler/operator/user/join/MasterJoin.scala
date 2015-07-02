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

import scala.reflect.ClassTag

import org.objectweb.asm.Type

import com.asakusafw.lang.compiler.analyzer.util.PropertyMapping
import com.asakusafw.spark.runtime.util.ValueOptionOps
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._
import com.asakusafw.vocabulary.operator.{ MasterJoin => MasterJoinOp }

trait MasterJoin extends JoinOperatorFragmentClassBuilder {

  val opInfo: OperatorInfo
  import opInfo._ // scalastyle:ignore

  val mappings: Seq[PropertyMapping]

  override def defFields(fieldDef: FieldDef): Unit = {
    super.defFields(fieldDef)
    fieldDef.newField("joinedDataModel", outputs(MasterJoinOp.ID_OUTPUT_JOINED).dataModelType)
  }

  override def initFields(mb: MethodBuilder): Unit = {
    super.initFields(mb)

    import mb._ // scalastyle:ignore
    thisVar.push().putField(
      "joinedDataModel",
      outputs(MasterJoinOp.ID_OUTPUT_JOINED).dataModelType,
      pushNew0(outputs(MasterJoinOp.ID_OUTPUT_JOINED).dataModelType))
  }

  override def join(mb: MethodBuilder, masterVar: Var, txVar: Var): Unit = {
    import mb._ // scalastyle:ignore
    block { ctrl =>
      masterVar.push().unlessNotNull {
        getOutputField(mb, outputs(MasterJoinOp.ID_OUTPUT_MISSED))
          .invokeV("add", txVar.push().asType(classOf[AnyRef].asType))
        ctrl.break()
      }

      val vars = Seq(masterVar, txVar)

      thisVar.push()
        .getField("joinedDataModel", outputs(MasterJoinOp.ID_OUTPUT_JOINED).dataModelType)
        .invokeV("reset")

      mappings.foreach { mapping =>
        val src = inputs.indexOf(mapping.getSourcePort)
        val srcVar = vars(src)
        val srcProperty =
          inputs(src).dataModelRef.findProperty(mapping.getSourceProperty)

        assert(mapping.getDestinationPort == outputs(MasterJoinOp.ID_OUTPUT_JOINED),
          "The destination port should be the same as the port for MasterJoinOp.ID_OUTPUT_JOINED: "
            + s"(${mapping.getDestinationPort}, ${outputs(MasterJoinOp.ID_OUTPUT_JOINED)})")
        val destProperty =
          outputs(MasterJoinOp.ID_OUTPUT_JOINED).dataModelRef
            .findProperty(mapping.getDestinationProperty)

        assert(srcProperty.getType.asType == destProperty.getType.asType,
          "The source and destination types should be the same: "
            + s"(${srcProperty.getType}, ${destProperty.getType}")

        getStatic(ValueOptionOps.getClass.asType, "MODULE$", ValueOptionOps.getClass.asType)
          .invokeV(
            "copy",
            srcVar.push()
              .invokeV(srcProperty.getDeclaration.getName, srcProperty.getType.asType),
            thisVar.push()
              .getField("joinedDataModel", outputs(MasterJoinOp.ID_OUTPUT_JOINED).dataModelType)
              .invokeV(destProperty.getDeclaration.getName, destProperty.getType.asType))
      }

      getOutputField(mb, outputs(MasterJoinOp.ID_OUTPUT_JOINED))
        .invokeV("add",
          thisVar.push()
            .getField("joinedDataModel", outputs(MasterJoinOp.ID_OUTPUT_JOINED).dataModelType)
            .asType(classOf[AnyRef].asType))
    }
  }
}
