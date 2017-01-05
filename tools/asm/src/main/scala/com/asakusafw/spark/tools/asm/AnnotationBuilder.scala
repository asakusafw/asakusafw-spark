/*
 * Copyright 2011-2017 Asakusa Framework Team.
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

import org.objectweb.asm.AnnotationVisitor

class AnnotationBuilder(av: AnnotationVisitor) {

  def newValue(name: String, value: Any): Unit = {
    av.visit(name, value)
  }

  def newAnnotation(name: String, desc: String)(implicit block: AnnotationBuilder => Unit): Unit = {
    val visitor = av.visitAnnotation(name, desc)
    try {
      block(new AnnotationBuilder(visitor))
    } finally {
      visitor.visitEnd()
    }
  }

  def newArray(name: String)(implicit block: AnnotationBuilder => Unit): Unit = {
    val visitor = av.visitArray(name)
    try {
      block(new AnnotationBuilder(visitor))
    } finally {
      visitor.visitEnd()
    }
  }

  def newEnum(name: String, desc: String, value: String): Unit = {
    av.visitEnum(name, desc, value)
  }
}
