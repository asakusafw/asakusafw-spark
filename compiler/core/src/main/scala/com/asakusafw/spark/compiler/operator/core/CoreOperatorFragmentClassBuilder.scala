package com.asakusafw.spark.compiler.operator
package core

import org.objectweb.asm.Type
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.runtime.model.DataModel
import com.asakusafw.spark.runtime.fragment.Fragment
import com.asakusafw.spark.tools.asm._

abstract class CoreOperatorFragmentClassBuilder(flowId: String, dataModelType: Type, childDataModelType: Type)
    extends FragmentClassBuilder(flowId, dataModelType) {

  override def defFields(fieldDef: FieldDef): Unit = {
    fieldDef.newFinalField("child", classOf[Fragment[_]].asType,
      new TypeSignatureBuilder()
        .newClassType(classOf[Fragment[_]].asType) {
          _.newTypeArgument(SignatureVisitor.INSTANCEOF, childDataModelType)
        }
        .build())
    fieldDef.newFinalField("childDataModel", childDataModelType)
  }

  override def defConstructors(ctorDef: ConstructorDef): Unit = {
    ctorDef.newInit(Seq(classOf[Fragment[_]].asType),
      new MethodSignatureBuilder()
        .newParameterType {
          _.newClassType(classOf[Fragment[_]].asType) {
            _.newTypeArgument(SignatureVisitor.INSTANCEOF, childDataModelType)
          }
        }
        .newVoidReturnType()
        .build()) { mb =>
        import mb._
        thisVar.push().invokeInit(superType)
        val childVar = `var`(classOf[Fragment[_]].asType, thisVar.nextLocal)
        thisVar.push().putField("child", classOf[Fragment[_]].asType, childVar.push())
        thisVar.push().putField("childDataModel", childDataModelType, pushNew0(childDataModelType))
      }
  }

  override def defMethods(methodDef: MethodDef): Unit = {
    super.defMethods(methodDef)

    methodDef.newMethod("add", Seq(dataModelType))(defAddMethod)

    methodDef.newMethod("add", Seq(classOf[AnyRef].asType)) { mb =>
      import mb._
      val resultVar = `var`(classOf[AnyRef].asType, thisVar.nextLocal)
      thisVar.push().invokeV("add", resultVar.push().cast(dataModelType))
      `return`()
    }

    methodDef.newMethod("reset", Seq.empty) { mb =>
      import mb._
      thisVar.push().getField("child", classOf[Fragment[_]].asType).invokeV("reset")
      `return`()
    }
  }

  def defAddMethod(mb: MethodBuilder): Unit
}
