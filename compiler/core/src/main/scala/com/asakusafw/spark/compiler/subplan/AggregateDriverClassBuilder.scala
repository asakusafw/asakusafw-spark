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

import com.asakusafw.spark.runtime.aggregation.Aggregation
import com.asakusafw.spark.runtime.driver.{ AggregateDriver, ShuffleKey }
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._

abstract class AggregateDriverClassBuilder(
  val flowId: String,
  val valueType: Type,
  val combinerType: Type)
    extends ClassBuilder(
      Type.getType(s"L${GeneratedClassPackageInternalName}/${flowId}/driver/AggregateDriver$$${AggregateDriverClassBuilder.nextId};"),
      new ClassSignatureBuilder()
        .newSuperclass {
          _.newClassType(classOf[AggregateDriver[_, _, _]].asType) {
            _
              .newTypeArgument(SignatureVisitor.INSTANCEOF, valueType)
              .newTypeArgument(SignatureVisitor.INSTANCEOF, combinerType)
              .newTypeArgument(SignatureVisitor.INSTANCEOF, Type.LONG_TYPE.boxed)
          }
        }
        .build(),
      classOf[AggregateDriver[_, _, _]].asType)
    with Branching with DriverName {

  override def defConstructors(ctorDef: ConstructorDef): Unit = {
    ctorDef.newInit(Seq(
      classOf[SparkContext].asType,
      classOf[Broadcast[Configuration]].asType,
      classOf[Map[Long, Broadcast[_]]].asType,
      classOf[Seq[RDD[(ShuffleKey, _)]]].asType,
      classOf[Seq[Boolean]].asType,
      classOf[Partitioner].asType)) { mb =>
      import mb._
      val scVar = `var`(classOf[SparkContext].asType, thisVar.nextLocal)
      val hadoopConfVar = `var`(classOf[Broadcast[Configuration]].asType, scVar.nextLocal)
      val broadcastsVar = `var`(classOf[Map[Long, Broadcast[_]]].asType, hadoopConfVar.nextLocal)
      val prevsVar = `var`(classOf[Seq[RDD[(ShuffleKey, _)]]].asType, broadcastsVar.nextLocal)
      val directionsVar = `var`(classOf[Seq[Boolean]].asType, prevsVar.nextLocal)
      val partVar = `var`(classOf[Partitioner].asType, directionsVar.nextLocal)

      thisVar.push().invokeInit(
        superType,
        scVar.push(),
        hadoopConfVar.push(),
        broadcastsVar.push(),
        prevsVar.push(),
        directionsVar.push(),
        partVar.push())

      initFields(mb)
    }
  }
}

object AggregateDriverClassBuilder {

  private[this] val curId: AtomicLong = new AtomicLong(0L)

  def nextId: Long = curId.getAndIncrement
}
