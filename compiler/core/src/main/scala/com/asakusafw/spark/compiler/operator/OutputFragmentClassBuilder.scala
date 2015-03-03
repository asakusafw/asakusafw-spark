package com.asakusafw.spark.compiler.operator

import org.objectweb.asm.Type
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.runtime.model.DataModel
import com.asakusafw.spark.runtime.fragment.OutputFragment
import com.asakusafw.spark.tools.asm._

class OutputFragmentClassBuilder(flowId: String, dataModelType: Type)
    extends FragmentClassBuilder(
      flowId,
      dataModelType,
      Some(OutputFragmentClassBuilder.signature(dataModelType)),
      classOf[OutputFragment[_]].asType) {

  override def defMethods(methodDef: MethodDef): Unit = {
    super.defMethods(methodDef)

    methodDef.newMethod("newDataModel", dataModelType, Seq.empty) { mb =>
      import mb._
      `return`(pushNew0(dataModelType))
    }

    methodDef.newMethod("newDataModel", classOf[DataModel[_]].asType, Seq.empty) { mb =>
      import mb._
      `return`(thisVar.push().invokeV("newDataModel", dataModelType))
    }
  }
}

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
