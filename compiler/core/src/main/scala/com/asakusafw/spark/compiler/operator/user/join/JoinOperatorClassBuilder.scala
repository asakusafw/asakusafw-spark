package com.asakusafw.spark.compiler
package operator
package user
package join

import java.util.{ List => JList }

import scala.collection.JavaConversions
import scala.reflect.ClassTag

import org.objectweb.asm.Type
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.spark.runtime.fragment.Fragment
import com.asakusafw.spark.runtime.operator.DefaultMasterSelection
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._

abstract class JoinOperatorClassBuilder(
  flowId: String)
    extends FragmentClassBuilder(
      flowId, classOf[Seq[Iterable[_]]].asType)
    with OperatorField
    with OutputFragments {

  def masterType: Type
  def txType: Type
  def masterSelection: Option[(String, Type)]

  def join(mb: MethodBuilder, ctrl: LoopControl, masterVar: Var, txVar: Var): Unit

  override def defFields(fieldDef: FieldDef): Unit = {
    defOperatorField(fieldDef)
    defOutputFields(fieldDef)
  }

  override def defConstructors(ctorDef: ConstructorDef): Unit = {
    ctorDef.newInit((0 until operatorOutputs.size).map(_ => classOf[Fragment[_]].asType),
      ((new MethodSignatureBuilder() /: operatorOutputs) {
        case (builder, output) =>
          builder.newParameterType {
            _.newClassType(classOf[Fragment[_]].asType) {
              _.newTypeArgument(SignatureVisitor.INSTANCEOF, output.getDataType.asType)
            }
          }
      })
        .newVoidReturnType()
        .build()) { mb =>
        import mb._
        thisVar.push().invokeInit(superType)
        initOutputFields(mb, thisVar.nextLocal)
      }
  }

  override def defMethods(methodDef: MethodDef): Unit = {
    super.defMethods(methodDef)

    methodDef.newMethod("add", Seq(classOf[Seq[Iterable[_]]].asType),
      new MethodSignatureBuilder()
        .newParameterType {
          _.newClassType(classOf[Seq[_]].asType) {
            _.newTypeArgument(SignatureVisitor.INSTANCEOF) {
              _.newClassType(classOf[Iterable[_]].asType) {
                _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[AnyRef].asType)
              }
            }
          }
        }
        .newVoidReturnType()
        .build()) { mb =>
        import mb._
        val groupsVar = `var`(classOf[Seq[Iterable[_]]].asType, thisVar.nextLocal)
        val mastersVar = getStatic(JavaConversions.getClass.asType, "MODULE$", JavaConversions.getClass.asType)
          .invokeV("seqAsJavaList", classOf[JList[_]].asType,
            groupsVar.push().invokeI(
              "apply", classOf[AnyRef].asType, ldc(0).box().asType(classOf[AnyRef].asType))
              .cast(classOf[Iterable[_]].asType)
              .invokeI("toSeq", classOf[Seq[_]].asType))
          .store(groupsVar.nextLocal)
        val txIterVar = groupsVar.push().invokeI(
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

    methodDef.newMethod("reset", Seq.empty) { mb =>
      import mb._
      resetOutputs(mb)
      `return`()
    }

    defGetOperator(methodDef)
  }
}
