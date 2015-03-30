package com.asakusafw.spark.compiler
package subplan

import java.util.concurrent.atomic.AtomicLong

import scala.reflect.ClassTag

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.objectweb.asm._
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.spark.runtime.driver.OutputDriver
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._

abstract class OutputDriverClassBuilder(
  val flowId: String,
  val dataModelType: Type)
    extends ClassBuilder(
      Type.getType(s"L${GeneratedClassPackageInternalName}/${flowId}/driver/OutputDriver$$${OutputDriverClassBuilder.nextId};"),
      Option(OutputDriverClassBuilder.signature(dataModelType)),
      classOf[OutputDriver[_, _]].asType) with DriverName {

  override def defConstructors(ctorDef: ConstructorDef): Unit = {
    super.defConstructors(ctorDef)

    ctorDef.newInit(Seq(
      classOf[SparkContext].asType,
      classOf[Broadcast[Configuration]].asType,
      classOf[Seq[RDD[_]]].asType)) { mb =>
      import mb._
      val scVar = `var`(classOf[SparkContext].asType, thisVar.nextLocal)
      val hadoopConfVar = `var`(classOf[Broadcast[Configuration]].asType, scVar.nextLocal)
      val prevsVar = `var`(classOf[Seq[RDD[_]]].asType, hadoopConfVar.nextLocal)

      thisVar.push().invokeInit(
        superType,
        scVar.push(),
        hadoopConfVar.push(),
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
        _.newClassType(classOf[OutputDriver[_, _]].asType) {
          _
            .newTypeArgument(SignatureVisitor.INSTANCEOF, dataModelType)
        }
      }
      .build()
  }
}
