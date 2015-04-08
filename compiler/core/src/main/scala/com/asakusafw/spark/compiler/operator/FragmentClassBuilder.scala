package com.asakusafw.spark.compiler
package operator

import java.util.concurrent.atomic.AtomicLong

import org.objectweb.asm.Type
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.spark.runtime.fragment.Fragment
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._

abstract class FragmentClassBuilder(
  val flowId: String,
  val dataModelType: Type)
    extends ClassBuilder(
      Type.getType(s"L${GeneratedClassPackageInternalName}/${flowId}/fragment/Fragment$$${FragmentClassBuilder.nextId};"),
      new ClassSignatureBuilder()
        .newSuperclass {
          _.newClassType(classOf[Fragment[_]].asType) {
            _.newTypeArgument(SignatureVisitor.INSTANCEOF, dataModelType)
          }
        }
        .build(),
      classOf[Fragment[_]].asType) {

  override def defMethods(methodDef: MethodDef): Unit = {
    super.defMethods(methodDef)

    methodDef.newMethod("add", Seq(classOf[AnyRef].asType)) { mb =>
      import mb._
      val resultVar = `var`(classOf[AnyRef].asType, thisVar.nextLocal)
      thisVar.push().invokeV("add", resultVar.push().cast(dataModelType))
      `return`()
    }

    methodDef.newMethod("add", Seq(dataModelType)) { mb =>
      import mb._
      defAddMethod(mb, `var`(dataModelType, thisVar.nextLocal))
    }
  }

  def defAddMethod(mb: MethodBuilder, dataModelVar: Var): Unit
}

object FragmentClassBuilder {

  private[this] val curId: AtomicLong = new AtomicLong(0L)

  def nextId: Long = curId.getAndIncrement
}
