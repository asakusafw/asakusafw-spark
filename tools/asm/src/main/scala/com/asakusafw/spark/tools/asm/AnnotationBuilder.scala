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
