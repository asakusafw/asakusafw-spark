package com.asakusafw.spark.compiler.subplan

import com.asakusafw.runtime.model.DataModel
import com.asakusafw.spark.tools.asm._

trait Branching
    extends ClassBuilder
    with BranchKeysField
    with PartitionersField
    with OrderingsField {

  abstract override def defFields(fieldDef: FieldDef): Unit = {
    defBranchKeysField(fieldDef)
    defPartitionersField(fieldDef)
    defOrderingsField(fieldDef)
  }

  abstract override def defConstructors(ctorDef: ConstructorDef): Unit = {
    ctorDef.newStaticInit { mb =>
      initBranchKeysField(mb)
      initPartitionersField(mb)
      initOrderingsField(mb)
    }
  }

  abstract override def defMethods(methodDef: MethodDef): Unit = {
    defBranchKeys(methodDef)
    defPartitioners(methodDef)
    defOrderings(methodDef)

    methodDef.newMethod("shuffleKey", classOf[DataModel[_]].asType,
      Seq(classOf[AnyRef].asType, classOf[DataModel[_]].asType)) { mb =>
        import mb._
        // TODO
        `return`(pushNull(classOf[DataModel[_]].asType))
      }
  }
}
