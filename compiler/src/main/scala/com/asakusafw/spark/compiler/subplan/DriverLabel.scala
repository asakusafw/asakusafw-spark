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
package com.asakusafw.spark.compiler.subplan

import com.asakusafw.lang.compiler.model.graph.Operator
import com.asakusafw.spark.tools.asm._

trait DriverLabel extends ClassBuilder {

  def label: String

  override def defMethods(methodDef: MethodDef): Unit = {
    super.defMethods(methodDef)

    methodDef.newMethod("label", classOf[String].asType, Seq.empty) { mb =>
      import mb._ // scalastyle:ignore
      `return`(ldc(label))
    }
  }
}