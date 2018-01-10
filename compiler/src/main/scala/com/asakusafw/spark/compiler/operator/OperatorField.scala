/*
 * Copyright 2011-2018 Asakusa Framework Team.
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
package com.asakusafw.spark.compiler.operator

import org.objectweb.asm.{ Opcodes, Type }

import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._

trait OperatorField extends ClassBuilder {

  def operatorType: Type

  override def defFields(fieldDef: FieldDef): Unit = {
    super.defFields(fieldDef)

    fieldDef.newField(Opcodes.ACC_PRIVATE | Opcodes.ACC_FINAL, "operator", operatorType)
  }

  def initOperatorField()(implicit mb: MethodBuilder): Unit = {
    val thisVar :: _ = mb.argVars
    thisVar.push().putField("operator", pushNew0(operatorType))
  }

  def getOperatorField()(implicit mb: MethodBuilder): Stack = {
    val thisVar :: _ = mb.argVars
    thisVar.push().getField("operator", operatorType)
  }
}
