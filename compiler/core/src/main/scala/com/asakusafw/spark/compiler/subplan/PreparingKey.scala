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
import com.asakusafw.spark.runtime.driver.{ BranchKey, ShuffleKey }
import com.asakusafw.spark.tools.asm._

trait PreparingKey extends ClassBuilder {

  def flowId: String

  def jpContext: JPContext

  def branchKeys: BranchKeysClassBuilder

  def shuffleKeyTypes: mutable.Set[Type]

  def subplanOutputs: Seq[SubPlan.Output]

  override def defMethods(methodDef: MethodDef): Unit = {

    methodDef.newMethod("shuffleKey", classOf[ShuffleKey].asType,
      Seq(classOf[BranchKey].asType, classOf[AnyRef].asType)) { mb =>
        import mb._
        val branchVar = `var`(classOf[BranchKey].asType, thisVar.nextLocal)
        val valueVar = `var`(classOf[AnyRef].asType, branchVar.nextLocal)
        `return`(thisVar.push()
          .invokeV("shuffleKey", classOf[ShuffleKey].asType,
            branchVar.push(),
            valueVar.push().cast(classOf[DataModel[_]].asType)))
      }

    methodDef.newMethod("shuffleKey", classOf[ShuffleKey].asType,
      Seq(classOf[BranchKey].asType, classOf[DataModel[_]].asType)) { mb =>
        import mb._
        val branchVar = `var`(classOf[BranchKey].asType, thisVar.nextLocal)
        val valueVar = `var`(classOf[DataModel[_]].asType, branchVar.nextLocal)
        subplanOutputs.sortBy(_.getOperator.getOriginalSerialNumber).foreach { output =>
          val op = output.getOperator
          Option(output.getAttribute(classOf[PartitioningParameters])).foreach { params =>
            val dataModelRef = jpContext.getDataModelLoader.load(op.getInput.getDataType)
            val group = params.getKey
            val dataModelType = dataModelRef.getDeclaration.asType

            val shuffleKeyType = ShuffleKeyClassBuilder.getOrCompile(jpContext)(
              flowId,
              dataModelType,
              group.getGrouping.map { grouping =>
                val property = dataModelRef.findProperty(grouping)
                (property.getDeclaration.getName, property.getType.asType)
              },
              group.getOrdering.map { ordering =>
                val property = dataModelRef.findProperty(ordering.getPropertyName)
                (property.getDeclaration.getName, property.getType.asType)
              })
            shuffleKeyTypes += shuffleKeyType

            branchVar.push().unlessNotEqual(
              getStatic(
                branchKeys.thisType,
                branchKeys.getField(op.getOriginalSerialNumber),
                classOf[BranchKey].asType)) {
                val shuffleKey = pushNew(shuffleKeyType)
                shuffleKey.dup().invokeInit(valueVar.push().cast(dataModelType))
                `return`(shuffleKey)
              }
          }
        }
        `return`(pushNull(classOf[ShuffleKey].asType))
      }
  }
}
