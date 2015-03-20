package com.asakusafw.spark.compiler
package subplan

import java.util.concurrent.atomic.AtomicLong

import scala.reflect.ClassTag

import org.objectweb.asm._
import org.objectweb.asm.signature.SignatureVisitor
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import com.asakusafw.spark.runtime.driver.OutputDriver
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._

abstract class OutputDriverClassBuilder(
  val flowId: String,
  val dataModelType: Type)
    extends ClassBuilder(
      Type.getType(s"L${GeneratedClassPackageInternalName}/${flowId}/driver/OutputDriver$$${OutputDriverClassBuilder.nextId};"),
      Option(OutputDriverClassBuilder.signature(dataModelType)),
      classOf[OutputDriver[_]].asType) with DriverName {

  override def defConstructors(ctorDef: ConstructorDef): Unit = {
    super.defConstructors(ctorDef)

    ctorDef.newInit(Seq(classOf[SparkContext].asType, classOf[Seq[RDD[_]]].asType)) { mb =>
      import mb._
      val scVar = `var`(classOf[SparkContext].asType, thisVar.nextLocal)
      val prevsVar = `var`(classOf[Seq[RDD[_]]].asType, scVar.nextLocal)
      thisVar.push().invokeInit(superType,
        scVar.push(),
        prevsVar.push(),
        getStatic(ClassTag.getClass.asType, "MODULE$", ClassTag.getClass.asType)
          .invokeV("apply", classOf[ClassTag[_]].asType, ldc(dataModelType).asType(classOf[Class[_]].asType)))
    }
  }
}

object OutputDriverClassBuilder {

  private[this] val curId: AtomicLong = new AtomicLong(0L)

  def nextId: Long = curId.getAndIncrement

  def signature(dataModelType: Type): String = {
    new ClassSignatureBuilder()
      .newSuperclass {
        _.newClassType(classOf[OutputDriver[_]].asType) {
          _
            .newTypeArgument(SignatureVisitor.INSTANCEOF, dataModelType)
        }
      }
      .build()
  }
}
