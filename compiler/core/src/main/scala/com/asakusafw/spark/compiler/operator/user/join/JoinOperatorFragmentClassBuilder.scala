package com.asakusafw.spark.compiler
package operator
package user
package join

import java.util.{ List => JList }

import scala.collection.JavaConversions

import org.objectweb.asm.Type

import com.asakusafw.lang.compiler.model.graph.OperatorOutput
import com.asakusafw.spark.runtime.operator.DefaultMasterSelection
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._

abstract class JoinOperatorFragmentClassBuilder(
  flowId: String,
  operatorType: Type,
  operatorOutputs: Seq[OperatorOutput],
  masterType: Type,
  txType: Type,
  masterSelection: Option[(String, Type)])
    extends UserOperatorFragmentClassBuilder(
      flowId, classOf[Seq[Iterable[_]]].asType, operatorType, operatorOutputs) {

  def join(mb: MethodBuilder, ctrl: LoopControl, masterVar: Var, txVar: Var): Unit

  override def defAddMethod(mb: MethodBuilder, dataModelVar: Var): Unit = {
    import mb._
    val mastersVar = getStatic(JavaConversions.getClass.asType, "MODULE$", JavaConversions.getClass.asType)
      .invokeV("seqAsJavaList", classOf[JList[_]].asType,
        dataModelVar.push().invokeI(
          "apply", classOf[AnyRef].asType, ldc(0).box().asType(classOf[AnyRef].asType))
          .cast(classOf[Iterable[_]].asType)
          .invokeI("toSeq", classOf[Seq[_]].asType))
      .store(dataModelVar.nextLocal)
    val txIterVar = dataModelVar.push().invokeI(
      "apply", classOf[AnyRef].asType, ldc(1).box().asType(classOf[AnyRef].asType))
      .cast(classOf[Iterable[_]].asType)
      .invokeI("iterator", classOf[Iterator[_]].asType)
      .store(mastersVar.nextLocal)
    loop { ctrl =>
      txIterVar.push().invokeI("hasNext", Type.BOOLEAN_TYPE).unlessTrue(ctrl.break())
      val txVar = txIterVar.push().invokeI("next", classOf[AnyRef].asType)
        .cast(txType).store(txIterVar.nextLocal)
      val selectedVar = (masterSelection match {
        case Some((name, t)) =>
          getOperatorField(mb).invokeV(name, t.getReturnType(), mastersVar.push(), txVar.push())
        case None =>
          getStatic(DefaultMasterSelection.getClass.asType, "MODULE$", DefaultMasterSelection.getClass.asType)
            .invokeV("select", classOf[AnyRef].asType, mastersVar.push(), txVar.push().asType(classOf[AnyRef].asType))
            .cast(masterType)
      }).store(txVar.nextLocal)
      join(mb, ctrl, selectedVar, txVar)
    }
    `return`()
  }
}
