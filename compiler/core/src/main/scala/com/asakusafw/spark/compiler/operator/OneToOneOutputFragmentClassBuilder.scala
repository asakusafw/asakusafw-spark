package com.asakusafw.spark.compiler
package operator

import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable

import org.objectweb.asm.Type
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.lang.compiler.api.JobflowProcessor.{ Context => JPContext }
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.spark.runtime.driver.PrepareKey
import com.asakusafw.spark.runtime.fragment.OneToOneOutputFragment
import com.asakusafw.spark.tools.asm._

class OneToOneOutputFragmentClassBuilder(
  flowId: String,
  branchKeyType: Type,
  dataModelType: Type,
  groupingKeyType: Type)
    extends ClassBuilder(
      Type.getType(s"L${GeneratedClassPackageInternalName}/${flowId}/fragment/OneToOneOutputFragment$$${OneToOneOutputFragmentClassBuilder.nextId};"),
      Some(OneToOneOutputFragmentClassBuilder.signature(branchKeyType, dataModelType, groupingKeyType)),
      classOf[OneToOneOutputFragment[_, _, _]].asType) {

  override def defConstructors(ctorDef: ConstructorDef): Unit = {
    ctorDef.newInit(Seq(branchKeyType, classOf[PrepareKey[_]].asType)) { mb =>
      import mb._
      val branchVar = `var`(branchKeyType, thisVar.nextLocal)
      val prepareKeyVar = `var`(classOf[PrepareKey[_]].asType, branchVar.nextLocal)
      thisVar.push().invokeInit(superType, branchVar.push().asType(classOf[AnyRef].asType), prepareKeyVar.push())
    }
  }

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

object OneToOneOutputFragmentClassBuilder {

  private[this] val curId: AtomicLong = new AtomicLong(0L)

  def nextId: Long = curId.getAndIncrement

  def signature(branchKeyType: Type, dataModelType: Type, groupingKeyType: Type): String = {
    new ClassSignatureBuilder()
      .newSuperclass {
        _.newClassType(classOf[OneToOneOutputFragment[_, _, _]].asType) {
          _.newTypeArgument(SignatureVisitor.INSTANCEOF, branchKeyType)
            .newTypeArgument(SignatureVisitor.INSTANCEOF, dataModelType)
            .newTypeArgument(SignatureVisitor.INSTANCEOF, groupingKeyType)
        }
      }
      .build()
  }

  private[this] val cache: mutable.Map[JPContext, mutable.Map[(String, Type, Type, Type), Type]] =
    mutable.WeakHashMap.empty

  def getOrCompile(
    flowId: String,
    branchKeyType: Type,
    dataModelType: Type,
    groupingKeyType: Type,
    jpContext: JPContext): Type = {
    cache.getOrElseUpdate(jpContext, mutable.Map.empty).getOrElseUpdate(
      (flowId, branchKeyType, dataModelType, groupingKeyType),
      jpContext.addClass(new OneToOneOutputFragmentClassBuilder(flowId, branchKeyType, dataModelType, groupingKeyType)))
  }
}
