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

import com.asakusafw.lang.compiler.model.graph.OperatorOutput
import com.asakusafw.runtime.flow.{ ArrayListBuffer, ListBuffer }
import com.asakusafw.spark.runtime.operator.DefaultMasterSelection
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._

trait ShuffledJoin extends JoinOperatorFragmentClassBuilder {

  val operatorInfo: OperatorInfo
  import operatorInfo._ // scalastyle:ignore

  override def defFields(fieldDef: FieldDef): Unit = {
    super.defFields(fieldDef)

    fieldDef.newField(
      Opcodes.ACC_PRIVATE | Opcodes.ACC_FINAL,
      "masters",
      classOf[ListBuffer[_]].asType,
      new TypeSignatureBuilder()
        .newClassType(classOf[ListBuffer[_]].asType) {
          _.newTypeArgument(SignatureVisitor.INSTANCEOF, masterType)
        }
        .build())
  }

  override def initFields(mb: MethodBuilder): Unit = {
    super.initFields(mb)

    import mb._ // scalastyle:ignore
    thisVar.push()
      .putField(
        "masters",
        classOf[ListBuffer[_]].asType,
        pushNew0(classOf[ArrayListBuffer[_]].asType))
  }

  override def defAddMethod(mb: MethodBuilder, dataModelVar: Var): Unit = {
    import mb._ // scalastyle:ignore
    val mastersVar = {
      val iter = dataModelVar.push().invokeI(
        "apply", classOf[AnyRef].asType, ldc(0).box().asType(classOf[AnyRef].asType))
        .cast(classOf[Iterator[_]].asType)
      val iterVar = iter.store(dataModelVar.nextLocal)
      val masters = thisVar.push().getField("masters", classOf[ListBuffer[_]].asType)
      val mastersVar = masters.store(iterVar.nextLocal)
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

    val txIterVar = dataModelVar.push().invokeI(
      "apply", classOf[AnyRef].asType, ldc(1).box().asType(classOf[AnyRef].asType))
      .cast(classOf[Iterator[_]].asType)
      .store(mastersVar.nextLocal)
    whileLoop(txIterVar.push().invokeI("hasNext", Type.BOOLEAN_TYPE)) { ctrl =>
      val txVar = txIterVar.push().invokeI("next", classOf[AnyRef].asType)
        .cast(txType).store(txIterVar.nextLocal)
      val selectedVar = (masterSelection match {
        case Some((name, t)) =>
          getOperatorField(mb)
            .invokeV(
              name,
              t.getReturnType(),
              ({ () => mastersVar.push() } +:
                { () => txVar.push() } +:
                arguments.map { argument =>
                  () => ldc(argument.value)(ClassTag(argument.resolveClass))
                }).zip(t.getArgumentTypes()).map {
                  case (s, t) => s().asType(t)
                }: _*)
        case None =>
          getStatic(
            DefaultMasterSelection.getClass.asType,
            "MODULE$",
            DefaultMasterSelection.getClass.asType)
            .invokeV(
              "select",
              classOf[AnyRef].asType,
              mastersVar.push().asType(classOf[JList[_]].asType),
              txVar.push().asType(classOf[AnyRef].asType))
            .cast(masterType)
      }).store(txVar.nextLocal)
      join(mb, selectedVar, txVar)
    }

    mastersVar.push().invokeI("shrink")

    `return`()
  }
}
