package com.asakusafw.spark.compiler.operator

import org.objectweb.asm.Type
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.spark.runtime.fragment.{ EdgeFragment, Fragment }
import com.asakusafw.spark.tools.asm._

class EdgeFragmentClassBuilder(dataModelType: Type)
    extends FragmentClassBuilder(
      dataModelType,
      Some(EdgeFragmentClassBuilder.signature(dataModelType)),
      classOf[EdgeFragment[_]].asType)
    with DefineNewDataModel {

  override def defConstructors(ctorDef: ConstructorDef): Unit = {
    ctorDef.newInit(Seq(classOf[Seq[Fragment[_]]].asType)) { mb =>
      import mb._
      val fragments = `var`(classOf[Seq[Fragment[_]]].asType, thisVar.nextLocal)
      thisVar.push().invokeInit(superType, fragments.push())
    }
  }
}

object EdgeFragmentClassBuilder {

  def signature(dataModelType: Type): String = {
    new ClassSignatureBuilder()
      .newSuperclass {
        _.newClassType(classOf[EdgeFragment[_]].asType) {
          _.newTypeArgument(SignatureVisitor.INSTANCEOF) {
            _.newClassType(dataModelType)
          }
        }
      }
      .build()
  }
}
