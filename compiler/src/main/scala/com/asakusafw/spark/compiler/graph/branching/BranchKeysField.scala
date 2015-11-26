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

import org.objectweb.asm.Opcodes
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.lang.compiler.planning.SubPlan
import com.asakusafw.spark.runtime.rdd.BranchKey
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._
import com.asakusafw.spark.tools.asm4s._

trait BranchKeysField extends ClassBuilder {

  implicit def context: BranchKeysField.Context

  def subplanOutputs: Seq[SubPlan.Output]

  override def defFields(fieldDef: FieldDef): Unit = {
    super.defFields(fieldDef)

    fieldDef.newField(
      Opcodes.ACC_PRIVATE | Opcodes.ACC_TRANSIENT,
      "branchKeys",
      classOf[Set[_]].asType,
      new TypeSignatureBuilder()
        .newClassType(classOf[Set[_]].asType) {
          _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[BranchKey].asType)
        }
        .build())
  }

  override def defMethods(methodDef: MethodDef): Unit = {
    super.defMethods(methodDef)

    methodDef.newMethod("branchKeys", classOf[Set[_]].asType, Seq.empty,
      new MethodSignatureBuilder()
        .newReturnType {
          _.newClassType(classOf[Set[_]].asType) {
            _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[BranchKey].asType)
          }
        }
        .build()) { implicit mb =>
        val thisVar :: _ = mb.argVars
        thisVar.push().getField("branchKeys", classOf[Set[_]].asType).unlessNotNull {
          thisVar.push().putField("branchKeys", initBranchKeys())
        }
        `return`(thisVar.push().getField("branchKeys", classOf[Set[_]].asType))
      }
  }

  def getBranchKeysField()(implicit mb: MethodBuilder): Stack = {
    val thisVar :: _ = mb.argVars
    thisVar.push().invokeV("branchKeys", classOf[Set[_]].asType)
  }

  private def initBranchKeys()(implicit mb: MethodBuilder): Stack = {
    buildSet { builder =>
      subplanOutputs.map(_.getOperator).sortBy(_.getSerialNumber).foreach { marker =>
        builder += context.branchKeys.getField(marker)
      }
    }
  }
}

object BranchKeysField {

  trait Context {

    def branchKeys: BranchKeys
  }
}
