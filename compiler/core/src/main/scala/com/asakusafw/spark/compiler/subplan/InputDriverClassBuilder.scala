package com.asakusafw.spark.compiler
package subplan

import java.util.concurrent.atomic.AtomicLong

import scala.reflect.ClassTag

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.objectweb.asm._
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.spark.runtime.driver.InputDriver
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._

abstract class InputDriverClassBuilder(
  val flowId: String,
  val dataModelType: Type)
    extends ClassBuilder(
      Type.getType(s"L${GeneratedClassPackageInternalName}/${flowId}/driver/InputDriver$$${InputDriverClassBuilder.nextId};"),
      new ClassSignatureBuilder()
        .newSuperclass {
          _.newClassType(classOf[InputDriver[_, _]].asType) {
            _
              .newTypeArgument(SignatureVisitor.INSTANCEOF, dataModelType)
              .newTypeArgument(SignatureVisitor.INSTANCEOF, Type.LONG_TYPE.boxed)
          }
        }
        .build(),
      classOf[InputDriver[_, _]].asType)
    with Branching with DriverName {

  override def defConstructors(ctorDef: ConstructorDef): Unit = {
    ctorDef.newInit(Seq(
      classOf[SparkContext].asType,
      classOf[Broadcast[Configuration]].asType,
      classOf[Map[Long, Broadcast[_]]].asType)) { mb =>
      import mb._
      val scVar = `var`(classOf[SparkContext].asType, thisVar.nextLocal)
      val hadoopConfVar = `var`(classOf[Broadcast[Configuration]].asType, scVar.nextLocal)
      val broadcastsVar = `var`(classOf[Map[Long, Broadcast[_]]].asType, hadoopConfVar.nextLocal)

      thisVar.push().invokeInit(
        superType,
        scVar.push(),
        hadoopConfVar.push(),
        broadcastsVar.push(),
        getStatic(ClassTag.getClass.asType, "MODULE$", ClassTag.getClass.asType)
          .invokeV("apply", classOf[ClassTag[_]].asType, ldc(dataModelType).asType(classOf[Class[_]].asType)))

      initFields(mb)
    }
  }
}

object InputDriverClassBuilder {

  private[this] val curId: AtomicLong = new AtomicLong(0L)

  def nextId: Long = curId.getAndIncrement
}
