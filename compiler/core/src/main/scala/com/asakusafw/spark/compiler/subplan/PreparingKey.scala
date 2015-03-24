package com.asakusafw.spark.compiler
package subplan

import scala.collection.mutable
import scala.collection.JavaConversions._
import scala.reflect.NameTransformer

import org.objectweb.asm.Type

import com.asakusafw.lang.compiler.api.JobflowProcessor.{ Context => JPContext }
import com.asakusafw.lang.compiler.planning.SubPlan
import com.asakusafw.lang.compiler.planning.spark.PartitioningParameters
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.spark.tools.asm._

trait PreparingKey
    extends ClassBuilder {

  def flowId: String

  def jpContext: JPContext

  def subplanOutputs: Seq[SubPlan.Output]

  override def defMethods(methodDef: MethodDef): Unit = {

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
        subplanOutputs.sortBy(_.getOperator.getOriginalSerialNumber).foreach { output =>
          val op = output.getOperator
          Option(output.getAttribute(classOf[PartitioningParameters])).foreach { params =>
            val dataModelRef = jpContext.getDataModelLoader.load(op.getInput.getDataType)
            val group = params.getKey

            branchVar.push().unlessNe(ldc(op.getOriginalSerialNumber)) {
              val dataModelVar = valueVar.push().cast(dataModelRef.getDeclaration.asType).store(valueVar.nextLocal)
              val builder = getStatic(Vector.getClass.asType, "MODULE$", Vector.getClass.asType)
                .invokeV("newBuilder", classOf[mutable.Builder[_, _]].asType)

              (group.getGrouping ++ group.getOrdering.map(_.getPropertyName)).foreach { grouping =>
                val property = dataModelRef.findProperty(grouping)
                builder.invokeI(
                  NameTransformer.encode("+="),
                  classOf[mutable.Builder[_, _]].asType,
                  dataModelVar.push().invokeV(property.getDeclaration.getName, property.getType.asType)
                    .asType(classOf[AnyRef].asType))
              }

              `return`(builder.invokeI("result", classOf[AnyRef].asType).cast(classOf[Vector[_]].asType))
            }
          }
        }
        `return`(pushNull(classOf[AnyRef].asType))
      }
  }
}
