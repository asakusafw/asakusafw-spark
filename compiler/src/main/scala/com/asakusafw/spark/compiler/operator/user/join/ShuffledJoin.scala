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

import java.util.{ List => JList }

import scala.reflect.ClassTag

import org.objectweb.asm.{ Opcodes, Type }
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.lang.compiler.model.graph.{ OperatorOutput, UserOperator }
import com.asakusafw.runtime.flow.{ ArrayListBuffer, ListBuffer }
import com.asakusafw.spark.compiler.spi.OperatorCompiler
import com.asakusafw.spark.runtime.operator.DefaultMasterSelection
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._
import com.asakusafw.spark.tools.asm4s._

trait ShuffledJoin
  extends JoinOperatorFragmentClassBuilder {

  implicit def context: OperatorCompiler.Context

  def operator: UserOperator

  override def defFields(fieldDef: FieldDef): Unit = {
    super.defFields(fieldDef)

    fieldDef.newField(
      Opcodes.ACC_PRIVATE | Opcodes.ACC_FINAL,
      "masters",
      classOf[ListBuffer[_]].asType,
      new TypeSignatureBuilder()
        .newClassType(classOf[ListBuffer[_]].asType) {
          _.newTypeArgument(SignatureVisitor.INSTANCEOF, masterType)
        })
  }

  override def initFields()(implicit mb: MethodBuilder): Unit = {
    super.initFields()

    val thisVar :: _ = mb.argVars

    thisVar.push()
      .putField(
        "masters",
        pushNew0(classOf[ArrayListBuffer[_]].asType).asType(classOf[ListBuffer[_]].asType))
  }

  override def defAddMethod(dataModelVar: Var)(implicit mb: MethodBuilder): Unit = {
    val thisVar :: _ = mb.argVars

    val mastersVar = {
      val iterVar = applySeq(dataModelVar.push(), ldc(0))
        .cast(classOf[Iterator[_]].asType)
        .store()
      val mastersVar = thisVar.push().getField("masters", classOf[ListBuffer[_]].asType).store()
      mastersVar.push().invokeI("begin")

      whileLoop(iterVar.push().invokeI("hasNext", Type.BOOLEAN_TYPE)) { ctrl =>
        mastersVar.push().invokeI("isExpandRequired", Type.BOOLEAN_TYPE).unlessFalse {
          mastersVar.push().invokeI("expand", pushNew0(masterType).asType(classOf[AnyRef].asType))
        }
        mastersVar.push().invokeI("advance", classOf[AnyRef].asType)
          .cast(masterType)
          .invokeV(
            "copyFrom",
            iterVar.push().invokeI("next", classOf[AnyRef].asType)
              .cast(masterType))
      }

      mastersVar.push().invokeI("end")
      mastersVar
    }

    val txIterVar =
      applySeq(dataModelVar.push(), ldc(1))
        .cast(classOf[Iterator[_]].asType)
        .store()
    whileLoop(txIterVar.push().invokeI("hasNext", Type.BOOLEAN_TYPE)) { ctrl =>
      val txVar = txIterVar.push().invokeI("next", classOf[AnyRef].asType)
        .cast(txType).store()
      val selectedVar = (masterSelection match {
        case Some((name, t)) =>
          getOperatorField()
            .invokeV(
              name,
              t.getReturnType(),
              ({ () => mastersVar.push() } +:
                { () => txVar.push() } +:
                operator.arguments.map { argument =>
                  Option(argument.value).map { value =>
                    () => ldc(value)(ClassTag(argument.resolveClass), implicitly)
                  }.getOrElse {
                    () => pushNull(argument.resolveClass.asType)
                  }
                }).zip(t.getArgumentTypes()).map {
                  case (s, t) => s().asType(t)
                }: _*)
        case None =>
          pushObject(DefaultMasterSelection)
            .invokeV(
              "select",
              classOf[AnyRef].asType,
              mastersVar.push().asType(classOf[JList[_]].asType),
              txVar.push().asType(classOf[AnyRef].asType))
            .cast(masterType)
      }).store()
      join(selectedVar, txVar)
    }

    mastersVar.push().invokeI("shrink")

    `return`()
  }
}
