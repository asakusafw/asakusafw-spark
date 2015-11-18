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

import java.util.concurrent.atomic.AtomicLong

import org.objectweb.asm.{ Opcodes, Type }
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.spark.compiler.operator.FragmentClassBuilder._
import com.asakusafw.spark.compiler.spi.OperatorCompiler
import com.asakusafw.spark.runtime.fragment.Fragment
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._

abstract class FragmentClassBuilder(
  val dataModelType: Type)(
    implicit context: OperatorCompiler.Context)
  extends ClassBuilder(
    Type.getType(
      s"L${GeneratedClassPackageInternalName}/${context.flowId}/fragment/Fragment$$${nextId};"),
    new ClassSignatureBuilder()
      .newSuperclass {
        _.newClassType(classOf[Fragment[_]].asType) {
          _.newTypeArgument(SignatureVisitor.INSTANCEOF, dataModelType)
        }
      }
      .build(),
    classOf[Fragment[_]].asType) {

  override def defFields(fieldDef: FieldDef): Unit = {
    super.defFields(fieldDef)

    fieldDef.newField(Opcodes.ACC_PRIVATE, "reset", Type.BOOLEAN_TYPE)
  }

  protected def initReset()(implicit mb: MethodBuilder): Unit = {
    import mb._ // scalastyle:ignore
    thisVar.push().putField("reset", Type.BOOLEAN_TYPE, ldc(true))
  }

  override def defMethods(methodDef: MethodDef): Unit = {
    super.defMethods(methodDef)

    methodDef.newMethod("add", Seq(classOf[AnyRef].asType)) { mb =>
      import mb._ // scalastyle:ignore
      val resultVar = `var`(classOf[AnyRef].asType, thisVar.nextLocal)
      thisVar.push().invokeV("add", resultVar.push().cast(dataModelType))
      `return`()
    }

    methodDef.newMethod("add", Seq(dataModelType)) { implicit mb =>
      import mb._ // scalastyle:ignore
      thisVar.push().putField("reset", Type.BOOLEAN_TYPE, ldc(false))
      defAddMethod(`var`(dataModelType, thisVar.nextLocal))
    }

    methodDef.newMethod("reset", Seq.empty)(defReset()(_))
  }

  def defAddMethod(dataModelVar: Var)(implicit mb: MethodBuilder): Unit
  def defReset()(implicit mb: MethodBuilder): Unit

  protected def unlessReset(b: => Unit)(implicit mb: MethodBuilder): Unit = {
    import mb._ // scalastyle:ignore
    thisVar.push().getField("reset", Type.BOOLEAN_TYPE).unlessTrue {
      b
      thisVar.push().putField("reset", Type.BOOLEAN_TYPE, ldc(true))
    }
  }
}

object FragmentClassBuilder {

  private[this] val curId: AtomicLong = new AtomicLong(0L)

  def nextId: Long = curId.getAndIncrement
}
