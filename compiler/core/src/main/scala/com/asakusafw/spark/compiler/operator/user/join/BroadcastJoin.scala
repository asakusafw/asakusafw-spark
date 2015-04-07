package com.asakusafw.spark.compiler
package operator
package user
package join

import java.util.{ List => JList }

import scala.collection.mutable
import scala.collection.JavaConversions
import scala.collection.JavaConversions._
import scala.reflect.NameTransformer

import org.apache.spark.broadcast.Broadcast
import org.objectweb.asm.Type
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.lang.compiler.api.JobflowProcessor.{ Context => JPContext }
import com.asakusafw.lang.compiler.model.graph.{ MarkerOperator, OperatorInput }
import com.asakusafw.lang.compiler.planning.PlanMarker
import com.asakusafw.spark.compiler.subplan.ShuffleKeyClassBuilder
import com.asakusafw.spark.runtime.driver.{ BroadcastId, ShuffleKey }
import com.asakusafw.spark.runtime.fragment.Fragment
import com.asakusafw.spark.runtime.operator.DefaultMasterSelection
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._

trait BroadcastJoin extends JoinOperatorFragmentClassBuilder {

  def jpContext: JPContext
  def shuffleKeyTypes: mutable.Set[Type]

  def masterInput: OperatorInput
  def txInput: OperatorInput

  override def defFields(fieldDef: FieldDef): Unit = {
    super.defFields(fieldDef)
    fieldDef.newField("masters", classOf[Map[_, _]].asType,
      new TypeSignatureBuilder()
        .newClassType(classOf[Map[_, _]].asType) {
          _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[ShuffleKey].asType)
            .newTypeArgument(SignatureVisitor.INSTANCEOF) {
              _.newClassType(classOf[Seq[_]].asType) {
                _.newTypeArgument(SignatureVisitor.INSTANCEOF, masterType)
              }
            }
        }
        .build())
  }

  override def initFields(mb: MethodBuilder): Unit = {
    import mb._
    val broadcastsVar = `var`(classOf[Map[BroadcastId, Broadcast[_]]].asType, thisVar.nextLocal)

    val masterSerialNumber: Long = {
      val opposites = masterInput.getOpposites
      assert(opposites.size == 1,
        s"The size of master inputs should be 1: ${opposites.size}")
      val opposite = opposites.head.getOwner
      assert(opposite.isInstanceOf[MarkerOperator],
        s"The master input should be marker operator: ${opposite}")
      assert(opposite.asInstanceOf[MarkerOperator].getAttribute(classOf[PlanMarker]) == PlanMarker.BROADCAST,
        s"The master input should be BROADCAST marker operator: ${
          opposite.asInstanceOf[MarkerOperator].getAttribute(classOf[PlanMarker])
        }")
      opposite.getOriginalSerialNumber
    }

    thisVar.push().putField(
      "masters",
      classOf[Map[_, _]].asType,
      getBroadcast(mb, masterSerialNumber)
        .invokeV("value", classOf[AnyRef].asType)
        .cast(classOf[Map[_, _]].asType))
  }

  override def defAddMethod(mb: MethodBuilder, dataModelVar: Var): Unit = {
    import mb._
    val keyVar = {
      val dataModelRef = jpContext.getDataModelLoader.load(txInput.getDataType)
      val group = txInput.getGroup

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

      val shuffleKey = pushNew(shuffleKeyType)
      shuffleKey.dup().invokeInit(dataModelVar.push())
      shuffleKey.invokeV("dropOrdering", classOf[ShuffleKey].asType).store(dataModelVar.nextLocal)
    }

    val mastersVar = getStatic(JavaConversions.getClass.asType, "MODULE$", JavaConversions.getClass.asType)
      .invokeV("seqAsJavaList", classOf[JList[_]].asType,
        thisVar.push().getField("masters", classOf[Map[_, _]].asType)
          .invokeI("apply", classOf[AnyRef].asType, keyVar.push().asType(classOf[AnyRef].asType))
          .cast(classOf[Seq[_]].asType))
      .store(keyVar.nextLocal)

    val selectedVar = (masterSelection match {
      case Some((name, t)) =>
        getOperatorField(mb)
          .invokeV(
            name,
            t.getReturnType(),
            mastersVar.push().asType(t.getArgumentTypes()(0)),
            dataModelVar.push().asType(t.getArgumentTypes()(1)))
      case None =>
        getStatic(DefaultMasterSelection.getClass.asType, "MODULE$", DefaultMasterSelection.getClass.asType)
          .invokeV("select", classOf[AnyRef].asType, mastersVar.push(), dataModelVar.push().asType(classOf[AnyRef].asType))
          .cast(masterType)
    }).store(mastersVar.nextLocal)

    val bVar = ldc(false).store(selectedVar.nextLocal)
    loop { ctrl =>
      bVar.push().unlessFalse(ctrl.break)
      ldc(true).store(bVar.local)
      join(mb, ctrl, selectedVar, dataModelVar)
    }
    `return`()
  }
}
