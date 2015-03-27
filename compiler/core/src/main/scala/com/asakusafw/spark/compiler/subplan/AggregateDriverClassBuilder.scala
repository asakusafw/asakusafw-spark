package com.asakusafw.spark.compiler
package subplan

import java.util.concurrent.atomic.AtomicLong

import scala.reflect.ClassTag

import org.apache.hadoop.conf.Configuration
import org.apache.spark.{ Partitioner, SparkContext }
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.objectweb.asm._
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.spark.runtime.driver.AggregateDriver
import com.asakusafw.spark.runtime.fragment.Aggregation
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._

abstract class AggregateDriverClassBuilder(
  val flowId: String,
  val valueType: Type,
  val combinerType: Type)
    extends ClassBuilder(
      Type.getType(s"L${GeneratedClassPackageInternalName}/${flowId}/driver/AggregateDriver$$${AggregateDriverClassBuilder.nextId};"),
      Option(AggregateDriverClassBuilder.signature(valueType, combinerType)),
      classOf[AggregateDriver[_, _, _, _]].asType)
    with Branching with DriverName {

  override def defConstructors(ctorDef: ConstructorDef): Unit = {
    ctorDef.newInit(Seq(
      classOf[SparkContext].asType,
      classOf[Broadcast[Configuration]].asType,
      classOf[Seq[RDD[_]]].asType,
      classOf[Partitioner].asType)) { mb =>
      import mb._
      val scVar = `var`(classOf[SparkContext].asType, thisVar.nextLocal)
      val hadoopConfVar = `var`(classOf[Broadcast[Configuration]].asType, scVar.nextLocal)
      val prevsVar = `var`(classOf[Seq[RDD[_]]].asType, hadoopConfVar.nextLocal)
      val partVar = `var`(classOf[Partitioner].asType, prevsVar.nextLocal)

      thisVar.push().invokeInit(
        superType,
        scVar.push(),
        hadoopConfVar.push(),
        prevsVar.push(),
        partVar.push(),
        getStatic(ClassTag.getClass.asType, "MODULE$", ClassTag.getClass.asType)
          .invokeV("apply", classOf[ClassTag[_]].asType, ldc(classOf[Seq[_]].asType).asType(classOf[Class[_]].asType)),
        getStatic(ClassTag.getClass.asType, "MODULE$", ClassTag.getClass.asType)
          .invokeV("apply", classOf[ClassTag[_]].asType, ldc(valueType).asType(classOf[Class[_]].asType)))

      initFields(mb)
    }
  }
}

object AggregateDriverClassBuilder {

  private[this] val curId: AtomicLong = new AtomicLong(0L)

  def nextId: Long = curId.getAndIncrement

  def signature(valueType: Type, combinerType: Type): String = {
    new ClassSignatureBuilder()
      .newSuperclass {
        _.newClassType(classOf[AggregateDriver[_, _, _, _]].asType) {
          _
            .newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[Seq[_]].asType)
            .newTypeArgument(SignatureVisitor.INSTANCEOF, valueType)
            .newTypeArgument(SignatureVisitor.INSTANCEOF, combinerType)
            .newTypeArgument(SignatureVisitor.INSTANCEOF, Type.LONG_TYPE.boxed)
        }
      }
      .build()
  }
}
