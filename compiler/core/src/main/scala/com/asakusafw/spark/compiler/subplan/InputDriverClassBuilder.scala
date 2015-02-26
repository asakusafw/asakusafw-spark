package com.asakusafw.spark.compiler.subplan

import java.util.concurrent.atomic.AtomicLong

import scala.reflect.ClassTag

import org.objectweb.asm._
import org.objectweb.asm.signature.SignatureVisitor
import org.apache.spark.SparkContext

import com.asakusafw.spark.runtime.driver.InputDriver
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._

abstract class InputDriverClassBuilder(
  val dataModelType: Type,
  val branchKeyType: Type)
    extends ClassBuilder(
      Type.getType(s"L${classOf[InputDriver[_, _]].asType.getInternalName}$$${InputDriverClassBuilder.nextId};"),
      Option(InputDriverClassBuilder.signature(dataModelType, branchKeyType)),
      classOf[InputDriver[_, _]].asType)
    with Branching {

  override def defConstructors(ctorDef: ConstructorDef): Unit = {
    super.defConstructors(ctorDef)

    ctorDef.newInit(Seq(classOf[SparkContext].asType)) { mb =>
      import mb._
      val scVar = `var`(classOf[SparkContext].asType, thisVar.nextLocal)
      thisVar.push().invokeInit(superType, scVar.push(),
        getStatic(ClassTag.getClass.asType, "MODULE$", ClassTag.getClass.asType)
          .invokeV("apply", classOf[ClassTag[_]].asType, ldc(dataModelType).asType(classOf[Class[_]].asType)))
    }
  }
}

object InputDriverClassBuilder {

  private[this] val curId: AtomicLong = new AtomicLong(0L)

  def nextId: Long = curId.getAndIncrement

  def signature(dataModelType: Type, branchKeyType: Type): String = {
    new ClassSignatureBuilder()
      .newSuperclass {
        _.newClassType(classOf[InputDriver[_, _]].asType) {
          _
            .newTypeArgument(SignatureVisitor.INSTANCEOF, dataModelType)
            .newTypeArgument(SignatureVisitor.INSTANCEOF, branchKeyType)
        }
      }
      .build()
  }
}
