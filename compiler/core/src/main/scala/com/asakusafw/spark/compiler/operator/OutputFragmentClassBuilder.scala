package com.asakusafw.spark.compiler.operator

import org.objectweb.asm.Type
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.spark.runtime.fragment.OutputFragment
import com.asakusafw.spark.tools.asm._

class OutputFragmentClassBuilder(dataModelType: Type)
  extends FragmentClassBuilder(
    dataModelType,
    Some(OutputFragmentClassBuilder.signature(dataModelType)),
    classOf[OutputFragment[_]].asType)
  with DefineNewDataModel

object OutputFragmentClassBuilder {

  def signature(dataModelType: Type): String = {
    new ClassSignatureBuilder()
      .newSuperclass {
        _.newClassType(classOf[OutputFragment[_]].asType) {
          _.newTypeArgument(SignatureVisitor.INSTANCEOF) {
            _.newClassType(dataModelType)
          }
        }
      }
      .build()
  }
}
