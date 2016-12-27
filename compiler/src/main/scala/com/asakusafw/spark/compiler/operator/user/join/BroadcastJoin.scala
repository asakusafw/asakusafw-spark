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

import scala.collection.JavaConversions._

import org.apache.spark.broadcast.{ Broadcast => Broadcasted }
import org.objectweb.asm.{ Opcodes, Type }
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.lang.compiler.model.graph.{ MarkerOperator, OperatorInput }
import com.asakusafw.lang.compiler.planning.PlanMarker
import com.asakusafw.runtime.core.GroupView
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.spark.compiler.spi.OperatorCompiler
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._
import com.asakusafw.spark.tools.asm4s._

trait BroadcastJoin
  extends JoinOperatorFragmentClassBuilder {

  implicit def context: OperatorCompiler.Context

  override def defMethods(methodDef: MethodDef): Unit = {
    super.defMethods(methodDef)

    methodDef.newMethod(
      Opcodes.ACC_PROTECTED,
      "keyElements",
      classOf[Array[AnyRef]].asType,
      Seq(classOf[DataModel[_]].asType)) { implicit mb =>
        val thisVar :: txVar :: _ = mb.argVars
        `return`(
          thisVar.push()
            .invokeV("keyElements", classOf[Array[AnyRef]].asType, txVar.push().cast(txType)))
      }

    methodDef.newMethod(
      Opcodes.ACC_PROTECTED,
      "keyElements",
      classOf[Array[AnyRef]].asType,
      Seq(txType)) { implicit mb =>
        val thisVar :: txVar :: _ = mb.argVars

        val dataModelRef = txInput.dataModelRef
        val group = txInput.getGroup

        `return`(
          buildArray(classOf[AnyRef].asType) { builder =>
            for {
              propertyName <- group.getGrouping
              property = dataModelRef.findProperty(propertyName)
            } {
              builder +=
                txVar.push().invokeV(
                  property.getDeclaration.getName, property.getType.asType)
            }
          })
      }

    methodDef.newMethod(
      Opcodes.ACC_PROTECTED,
      "masters",
      classOf[GroupView[_]].asType,
      Seq.empty) { implicit mb =>
        `return`(getViewField(masterInput))
      }
  }
}
