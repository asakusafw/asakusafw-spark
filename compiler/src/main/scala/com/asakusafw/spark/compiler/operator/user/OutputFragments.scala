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

import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.lang.compiler.model.graph.OperatorOutput
import com.asakusafw.spark.runtime.fragment.Fragment
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._

trait OutputFragments extends FragmentClassBuilder {

  def operatorOutputs: Seq[OperatorOutput]

  override def defFields(fieldDef: FieldDef): Unit = {
    super.defFields(fieldDef)

    operatorOutputs.foreach { output =>
      fieldDef.newFinalField(output.getName, classOf[Fragment[_]].asType,
        new TypeSignatureBuilder()
          .newClassType(classOf[Fragment[_]].asType) {
            _.newTypeArgument(SignatureVisitor.INSTANCEOF, output.getDataType.asType)
          })
    }
  }

  def initOutputFields(vars: Seq[Var])(implicit mb: MethodBuilder): Unit = {
    val thisVar :: _ = mb.argVars
    vars.zip(operatorOutputs).foreach {
      case (childVar, output) =>
        assert(childVar.`type` == classOf[Fragment[_]].asType)
        thisVar.push().putField(output.getName, childVar.push())
    }
  }

  def getOutputField(output: OperatorOutput)(implicit mb: MethodBuilder): Stack = {
    val thisVar :: _ = mb.argVars
    thisVar.push().getField(output.getName, classOf[Fragment[_]].asType)
  }

  def defReset()(implicit mb: MethodBuilder): Unit = {
    val thisVar :: _ = mb.argVars
    unlessReset {
      operatorOutputs.foreach { output =>
        thisVar.push().getField(output.getName, classOf[Fragment[_]].asType).invokeV("reset")
      }
    }
    `return`()
  }
}
