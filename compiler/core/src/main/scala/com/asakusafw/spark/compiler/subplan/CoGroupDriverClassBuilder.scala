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

import com.asakusafw.lang.compiler.model.graph.MarkerOperator
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.spark.runtime.driver.{ BroadcastId, CoGroupDriver, ShuffleKey }
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._

abstract class CoGroupDriverClassBuilder(
  val flowId: String,
  val groupingKeyType: Type)
    extends ClassBuilder(
      Type.getType(s"L${GeneratedClassPackageInternalName}/${flowId}/driver/CoGroupDriver$$${CoGroupDriverClassBuilder.nextId};"),
      classOf[CoGroupDriver].asType)
    with Branching with DriverName {

  override def defConstructors(ctorDef: ConstructorDef): Unit = {
    ctorDef.newInit(Seq(
      classOf[SparkContext].asType,
      classOf[Broadcast[Configuration]].asType,
      classOf[Map[BroadcastId, Broadcast[_]]].asType,
      classOf[Seq[(Seq[RDD[(ShuffleKey, _)]], Option[Seq[Boolean]])]].asType,
      classOf[Partitioner].asType)) { mb =>
      import mb._
      val scVar = `var`(classOf[SparkContext].asType, thisVar.nextLocal)
      val hadoopConfVar = `var`(classOf[Broadcast[Configuration]].asType, scVar.nextLocal)
      val broadcastsVar = `var`(classOf[Map[BroadcastId, Broadcast[_]]].asType, hadoopConfVar.nextLocal)
      val inputsVar = `var`(classOf[Seq[_]].asType, broadcastsVar.nextLocal)
      val partVar = `var`(classOf[Partitioner].asType, inputsVar.nextLocal)

      thisVar.push().invokeInit(
        superType,
        scVar.push(),
        hadoopConfVar.push(),
        broadcastsVar.push(),
        inputsVar.push(),
        partVar.push())

      initFields(mb)
    }
  }
}

object CoGroupDriverClassBuilder {

  private[this] val curId: AtomicLong = new AtomicLong(0L)

  def nextId: Long = curId.getAndIncrement
}
