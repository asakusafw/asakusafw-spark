/*
 * Copyright 2011-2015 Asakusa Framework Team.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.asakusafw.spark.compiler
package subplan

import java.util.concurrent.atomic.AtomicLong

import scala.concurrent.Future
import scala.reflect.ClassTag

import org.apache.hadoop.conf.Configuration
import org.apache.spark.{ Partitioner, SparkContext }
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.objectweb.asm._
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.spark.compiler.subplan.AggregateDriverClassBuilder._
import com.asakusafw.spark.runtime.aggregation.Aggregation
import com.asakusafw.spark.runtime.driver.{ AggregateDriver, BroadcastId, ShuffleKey }
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._

abstract class AggregateDriverClassBuilder(
  val flowId: String,
  val valueType: Type,
  val combinerType: Type)
  extends ClassBuilder(
    Type.getType(
      s"L${GeneratedClassPackageInternalName}/${flowId}/driver/AggregateDriver$$${nextId};"),
    new ClassSignatureBuilder()
      .newSuperclass {
        _.newClassType(classOf[AggregateDriver[_, _]].asType) {
          _
            .newTypeArgument(SignatureVisitor.INSTANCEOF, valueType)
            .newTypeArgument(SignatureVisitor.INSTANCEOF, combinerType)
        }
      }
      .build(),
    classOf[AggregateDriver[_, _]].asType)
  with Branching with DriverLabel {

  override def defConstructors(ctorDef: ConstructorDef): Unit = {
    ctorDef.newInit(Seq(
      classOf[SparkContext].asType,
      classOf[Broadcast[Configuration]].asType,
      classOf[Map[BroadcastId, Future[Broadcast[_]]]].asType,
      classOf[Seq[Future[RDD[(ShuffleKey, _)]]]].asType,
      classOf[Option[Ordering[ShuffleKey]]].asType,
      classOf[Partitioner].asType),
      new MethodSignatureBuilder()
        .newParameterType(classOf[SparkContext].asType)
        .newParameterType {
          _.newClassType(classOf[Broadcast[_]].asType) {
            _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[Configuration].asType)
          }
        }
        .newParameterType {
          _.newClassType(classOf[Map[_, _]].asType) {
            _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[BroadcastId].asType)
              .newTypeArgument(SignatureVisitor.INSTANCEOF) {
                _.newClassType(classOf[Future[_]].asType) {
                  _.newTypeArgument(SignatureVisitor.INSTANCEOF) {
                    _.newClassType(classOf[Broadcast[_]].asType) {
                      _.newTypeArgument()
                    }
                  }
                }
              }
          }
        }
        .newParameterType {
          _.newClassType(classOf[Seq[_]].asType) {
            _.newTypeArgument(SignatureVisitor.INSTANCEOF) {
              _.newClassType(classOf[Future[_]].asType) {
                _.newTypeArgument(SignatureVisitor.INSTANCEOF) {
                  _.newClassType(classOf[RDD[_]].asType) {
                    _.newTypeArgument(SignatureVisitor.INSTANCEOF) {
                      _.newClassType(classOf[(_, _)].asType) {
                        _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[ShuffleKey].asType)
                          .newTypeArgument()
                      }
                    }
                  }
                }
              }
            }
          }
        }
        .newParameterType {
          _.newClassType(classOf[Option[_]].asType) {
            _.newTypeArgument(SignatureVisitor.INSTANCEOF) {
              _.newClassType(classOf[Ordering[_]].asType) {
                _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[ShuffleKey].asType)
              }
            }
          }
        }
        .newParameterType(classOf[Partitioner].asType)
        .newVoidReturnType()
        .build()) { mb =>
        import mb._ // scalastyle:ignore
        val scVar =
          `var`(classOf[SparkContext].asType, thisVar.nextLocal)
        val hadoopConfVar =
          `var`(classOf[Broadcast[Configuration]].asType, scVar.nextLocal)
        val broadcastsVar =
          `var`(classOf[Map[BroadcastId, Future[Broadcast[_]]]].asType, hadoopConfVar.nextLocal)
        val prevsVar =
          `var`(classOf[Seq[Future[RDD[(ShuffleKey, _)]]]].asType, broadcastsVar.nextLocal)
        val sortVar =
          `var`(classOf[Option[Ordering[ShuffleKey]]].asType, prevsVar.nextLocal)
        val partVar =
          `var`(classOf[Partitioner].asType, sortVar.nextLocal)

        thisVar.push().invokeInit(
          superType,
          scVar.push(),
          hadoopConfVar.push(),
          broadcastsVar.push(),
          prevsVar.push(),
          sortVar.push(),
          partVar.push())
      }
  }
}

object AggregateDriverClassBuilder {

  private[this] val curId: AtomicLong = new AtomicLong(0L)

  def nextId: Long = curId.getAndIncrement
}
