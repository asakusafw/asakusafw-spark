package com.asakusafw.spark.tools.asm

import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner

import org.objectweb.asm.Opcodes._
import org.objectweb.asm.Type
import org.objectweb.asm.signature.SignatureReader
import org.objectweb.asm.signature.SignatureVisitor
import org.objectweb.asm.util.TraceSignatureVisitor

@RunWith(classOf[JUnitRunner])
class SignatureBuilderSpecTest extends SignatureBuilderSpec

class SignatureBuilderSpec extends FlatSpec {

  "SignatureBuilder" should "build class signature" in {
    val signature = new ClassSignatureBuilder()
      .newFormalTypeParameter("A") {
        _.newClassBound(classOf[AnyRef].asType)
      }
      .newSuperclass(classOf[AnyRef].asType)
      .newInterface {
        _.newClassType(classOf[Comparable[_]].asType) {
          _.newTypeArgument(SignatureVisitor.INSTANCEOF) {
            _.newTypeVariable("A")
          }
        }
      }.build()
    assert(signature === "<A:Ljava/lang/Object;>Ljava/lang/Object;Ljava/lang/Comparable<TA;>;")

    val trace = new TraceSignatureVisitor(ACC_PUBLIC)
    new SignatureReader(signature).accept(trace)
    assert(trace.getDeclaration() == "<A> implements java.lang.Comparable<A>")
  }

  it should "build method signature" in {
    val signature = new MethodSignatureBuilder()
      .newFormalTypeParameter("A") {
        _
          .newClassBound(classOf[AnyRef].asType)
          .newInterfaceBound(classOf[Serializable].asType)
      }
      .newVoidReturnType().build()
    assert(signature === "<A:Ljava/lang/Object;:Lscala/Serializable;>()V")

    val trace = new TraceSignatureVisitor(ACC_PUBLIC)
    new SignatureReader(signature).accept(trace)
    assert(trace.getDeclaration() == "<A extends scala.Serializable>()")
  }
}
