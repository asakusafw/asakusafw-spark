package com.asakusafw.spark.compiler
package operator

import scala.collection.mutable

import org.objectweb.asm.Type
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.lang.compiler.api.JobflowProcessor.{ Context => JPContext }
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.spark.runtime.fragment.{ EdgeFragment, Fragment }
import com.asakusafw.spark.tools.asm._

class EdgeFragmentClassBuilder(flowId: String, dataModelType: Type)
    extends FragmentClassBuilder(
      flowId,
      dataModelType,
      Some(EdgeFragmentClassBuilder.signature(dataModelType)),
      classOf[EdgeFragment[_]].asType) {

  override def defConstructors(ctorDef: ConstructorDef): Unit = {
    ctorDef.newInit(Seq(classOf[Seq[Fragment[_]]].asType)) { mb =>
      import mb._
      val childrenVar = `var`(classOf[Seq[Fragment[_]]].asType, thisVar.nextLocal)
      thisVar.push().invokeInit(superType, childrenVar.push())
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

object EdgeFragmentClassBuilder {

  def signature(dataModelType: Type): String = {
    new ClassSignatureBuilder()
      .newSuperclass {
        _.newClassType(classOf[EdgeFragment[_]].asType) {
          _.newTypeArgument(SignatureVisitor.INSTANCEOF) {
            _.newClassType(dataModelType)
          }
        }
      }
      .build()
  }

  private[this] val cache: mutable.Map[JPContext, mutable.Map[(String, Type), Type]] =
    mutable.WeakHashMap.empty

  def getOrCompile(
    flowId: String,
    dataModelType: Type,
    jpContext: JPContext): Type = {
    cache.getOrElseUpdate(jpContext, mutable.Map.empty).getOrElseUpdate(
      (flowId, dataModelType), {
        jpContext.addClass(new EdgeFragmentClassBuilder(flowId, dataModelType))
      })
  }
}
