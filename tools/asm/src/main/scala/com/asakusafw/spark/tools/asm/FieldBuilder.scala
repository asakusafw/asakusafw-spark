package com.asakusafw.spark.tools.asm

import org.objectweb.asm.Attribute
import org.objectweb.asm.FieldVisitor

class FieldBuilder(fv: FieldVisitor) {

  def newAnnotation(desc: String, visible: Boolean)(implicit block: AnnotationBuilder => Unit): Unit = {
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
