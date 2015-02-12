package com.asakusafw.spark.compiler.operator

import com.asakusafw.runtime.model.DataModel
import com.asakusafw.spark.tools.asm._

trait DefineNewDataModel extends FragmentClassBuilder {

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
