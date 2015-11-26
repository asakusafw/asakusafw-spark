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
package com.asakusafw.spark.tools.asm4s

import org.objectweb.asm.{ Opcodes, Type }

import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._

case class MixIn(traitType: Type, fields: Seq[MixIn.FieldDef], methods: Seq[MixIn.MethodDef])

object MixIn {

  case class FieldDef(access: Int, name: String, t: Type)

  object FieldDef {

    def apply(name: String, t: Type): FieldDef = {
      FieldDef(0, name, t)
    }
  }

  case class MethodDef(name: String, t: Type)

  object MethodDef {

    def apply(name: String, retType: Type, argumentTypes: Type*): MethodDef = {
      MethodDef(name, Type.getMethodType(retType, argumentTypes: _*))
    }
  }
}

trait Mixing extends ClassBuilder {

  import MixIn._ // scalastyle:ignore

  def mixins: Seq[MixIn]

  override def interfaceTypes: Seq[Type] = {
    super.interfaceTypes ++ mixins.map(_.traitType)
  }

  override def defFields(fieldDef: FieldDef): Unit = {
    super.defFields(fieldDef)

    for {
      mixin <- mixins
      FieldDef(access, field, t) <- mixin.fields
    } {
      fieldDef.newField(
        Opcodes.ACC_PRIVATE | access,
        fieldName(mixin.traitType, field),
        t)
    }
  }

  def initMixIns()(implicit mb: MethodBuilder): Unit = {
    val thisVar :: _ = mb.argVars
    for {
      mixin <- mixins
    } {
      invokeStatic(
        traitClassType(mixin.traitType),
        "$init$",
        thisVar.push().asType(mixin.traitType))
    }
  }

  override def defMethods(methodDef: MethodDef): Unit = {
    super.defMethods(methodDef)

    for {
      mixin <- mixins
    } {
      for {
        FieldDef(access, field, t) <- mixin.fields
      } {
        methodDef.newMethod(fieldName(mixin.traitType, field), t, Seq.empty) { implicit mb =>
          val thisVar :: _ = mb.argVars
          `return`(thisVar.push().getField(fieldName(mixin.traitType, field), t))
        }

        methodDef.newMethod(
          if ((access & Opcodes.ACC_FINAL) == 0) {
            fieldAssignerName(mixin.traitType, field)
          } else {
            fieldSetterName(mixin.traitType, field)
          },
          Seq(t)) { implicit mb =>
            val thisVar :: valueVar :: _ = mb.argVars
            thisVar.push().putField(fieldName(mixin.traitType, field), valueVar.push())
            `return`()
          }
      }

      for {
        MethodDef(method, t) <- mixin.methods
      } {
        methodDef.newMethod(method, t) { implicit mb =>
          val thisVar :: vars = mb.argVars

          `return`(
            invokeStatic(
              traitClassType(mixin.traitType),
              method,
              t.getReturnType,
              thisVar.push().asType(mixin.traitType) +: vars.map(_.push()): _*))
        }
      }
    }
  }

  private def traitName(traitType: Type): String = {
    traitType.getInternalName.replace('/', '$')
  }

  private def traitClassType(traitType: Type): Type = {
    Type.getType("L" + traitType.getInternalName + "$class;")
  }

  private def fieldName(traitType: Type, field: String): String = {
    traitName(traitType) + "$$" + field
  }

  private def fieldAssignerName(traitType: Type, field: String): String = {
    fieldName(traitType, field) + "_$eq"
  }

  private def fieldSetterName(traitType: Type, field: String): String = {
    traitName(traitType) + "$_setter_$" + fieldAssignerName(traitType, field)
  }
}
