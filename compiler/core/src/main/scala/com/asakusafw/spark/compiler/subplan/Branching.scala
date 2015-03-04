package com.asakusafw.spark.compiler.subplan

import org.objectweb.asm.Type
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.runtime.model.DataModel
import com.asakusafw.spark.tools.asm._

trait Branching
    extends ClassBuilder
    with BranchKeysField
    with PartitionersField
    with OrderingsField {

  override def defFields(fieldDef: FieldDef): Unit = {
    defBranchKeysField(fieldDef)
    defPartitionersField(fieldDef)
    defOrderingsField(fieldDef)
  }

  def initFields(mb: MethodBuilder): Unit = {
    initBranchKeysField(mb)
    initPartitionersField(mb)
    initOrderingsField(mb)
  }

  override def defMethods(methodDef: MethodDef): Unit = {
    defBranchKeys(methodDef)
    defPartitioners(methodDef)
    defOrderings(methodDef)

    methodDef.newMethod("shuffleKey", classOf[AnyRef].asType,
      Seq(classOf[AnyRef].asType, classOf[DataModel[_]].asType)) { mb =>
        import mb._
        val branchVar = `var`(classOf[AnyRef].asType, thisVar.nextLocal)
        val valueVar = `var`(classOf[DataModel[_]].asType, branchVar.nextLocal)
        `return`(thisVar.push()
          .invokeV("shuffleKey", classOf[AnyRef].asType,
            branchVar.push().cast(Type.LONG_TYPE.boxed),
            valueVar.push()))
      }

    methodDef.newMethod("shuffleKey", classOf[AnyRef].asType,
      Seq(Type.LONG_TYPE.boxed, classOf[DataModel[_]].asType),
      new MethodSignatureBuilder()
        .newFormalTypeParameter("U", classOf[AnyRef].asType)
        .newParameterType(Type.LONG_TYPE.boxed)
        .newParameterType(classOf[DataModel[_]].asType)
        .newReturnType {
          _.newTypeVariable("U")
        }
        .build()) { mb =>
        import mb._
        // TODO
        `return`(pushNull(classOf[AnyRef].asType))
      }
  }
}
