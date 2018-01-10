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
package com.asakusafw.spark.compiler.operator.user.join

import com.asakusafw.runtime.model.DataModel
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._
import com.asakusafw.spark.tools.asm4s._

trait ShuffledJoin
  extends JoinOperatorFragmentClassBuilder {

  override def defMethods(methodDef: MethodDef): Unit = {
    super.defMethods(methodDef)

    methodDef.newMethod(
      "newMasterDataModel",
      classOf[DataModel[_]].asType,
      Seq.empty,
      new MethodSignatureBuilder()
        .newReturnType {
          _.newClassType(classOf[DataModel[_]].asType) {
            _.newTypeArgument()
          }
        }) { implicit mb =>
        val thisVar :: _ = mb.argVars
        `return`(thisVar.push().invokeV("newMasterDataModel", masterType))
      }

    methodDef.newMethod("newMasterDataModel", masterType, Seq.empty) { implicit mb =>
      `return`(pushNew0(masterType))
    }
  }
}
