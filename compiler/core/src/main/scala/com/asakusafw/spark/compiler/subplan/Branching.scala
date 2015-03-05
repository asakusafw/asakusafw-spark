package com.asakusafw.spark.compiler
package subplan

import scala.collection.mutable
import scala.collection.JavaConversions._
import scala.reflect.NameTransformer

import org.objectweb.asm.Type
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.lang.compiler.model.graph.MarkerOperator
import com.asakusafw.lang.compiler.planning.spark.PartinioningParameters
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.spark.tools.asm._

trait Branching
    extends ClassBuilder
    with BranchKeysField
    with PartitionersField
    with OrderingsField {

  def outputMarkers: Seq[MarkerOperator]

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
            branchVar.push().cast(Type.LONG_TYPE.boxed).unbox(),
            valueVar.push()))
      }

    methodDef.newMethod("shuffleKey", classOf[AnyRef].asType,
      Seq(Type.LONG_TYPE, classOf[DataModel[_]].asType),
      new MethodSignatureBuilder()
        .newFormalTypeParameter("U", classOf[AnyRef].asType)
        .newParameterType(Type.LONG_TYPE)
        .newParameterType(classOf[DataModel[_]].asType)
        .newReturnType {
          _.newTypeVariable("U")
        }
        .build()) { mb =>
        import mb._
        val branchVar = `var`(Type.LONG_TYPE, thisVar.nextLocal)
        val valueVar = `var`(classOf[DataModel[_]].asType, branchVar.nextLocal)
        outputMarkers.sortBy(_.getOriginalSerialNumber).foreach { op =>
          Option(op.getAttribute(classOf[PartinioningParameters])).foreach { params =>
            val dataModelRef = jpContext.getDataModelLoader.load(op.getInput.getDataType)
            val group = params.getGroup

            branchVar.push().unlessNe(ldc(op.getOriginalSerialNumber)) {
              val dataModelVar = valueVar.push().cast(dataModelRef.getDeclaration.asType).store(valueVar.nextLocal)
              val builder = getStatic(Seq.getClass.asType, "MODULE$", Seq.getClass.asType)
                .invokeV("newBuilder", classOf[mutable.Builder[_, _]].asType)

              (group.getGrouping ++ group.getOrdering.map(_.getPropertyName)).foreach { grouping =>
                val property = dataModelRef.findProperty(grouping)
                builder.invokeI(
                  NameTransformer.encode("+="),
                  classOf[mutable.Builder[_, _]].asType,
                  dataModelVar.push().invokeV(property.getDeclaration.getName, property.getType.asType)
                    .asType(classOf[AnyRef].asType))
              }

              `return`(builder.invokeI("result", classOf[AnyRef].asType).cast(classOf[Seq[_]].asType))
            }
          }
        }
        `return`(pushNull(classOf[AnyRef].asType))
      }
  }
}
