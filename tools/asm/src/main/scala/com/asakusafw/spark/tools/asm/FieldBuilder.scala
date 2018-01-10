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
package com.asakusafw.spark.tools.asm

import org.objectweb.asm.Attribute
import org.objectweb.asm.FieldVisitor

class FieldBuilder(fv: FieldVisitor) {

  def newAnnotation(
    desc: String,
    visible: Boolean)(implicit block: AnnotationBuilder => Unit): Unit = {
    val av = fv.visitAnnotation(desc, visible)
    try {
      block(new AnnotationBuilder(av))
    } finally {
      av.visitEnd()
    }
  }

  def newAttribute(attr: Attribute): Unit = {
    fv.visitAttribute(attr)
  }
}
