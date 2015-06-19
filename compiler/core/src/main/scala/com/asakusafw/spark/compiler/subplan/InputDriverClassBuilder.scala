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
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.objectweb.asm._
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.spark.runtime.driver.{ BroadcastId, InputDriver }
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._

abstract class InputDriverClassBuilder(
  val flowId: String,
  val keyType: Type,
  val valueType: Type,
  val inputFormatType: Type)
    extends ClassBuilder(
      Type.getType(s"L${GeneratedClassPackageInternalName}/${flowId}/driver/InputDriver$$${InputDriverClassBuilder.nextId};"),
      new ClassSignatureBuilder()
        .newSuperclass {
          _.newClassType(classOf[InputDriver[_, _, _]].asType) {
            _.newTypeArgument(SignatureVisitor.INSTANCEOF, keyType)
              .newTypeArgument(SignatureVisitor.INSTANCEOF, valueType)
              .newTypeArgument(SignatureVisitor.INSTANCEOF, inputFormatType)
          }
        }
        .build(),
      classOf[InputDriver[_, _, _]].asType)
    with Branching with DriverLabel {

  override def defConstructors(ctorDef: ConstructorDef): Unit = {
    ctorDef.newInit(Seq(
      classOf[SparkContext].asType,
      classOf[Broadcast[Configuration]].asType,
      classOf[Map[BroadcastId, Future[Broadcast[_]]]].asType),
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
        .newVoidReturnType()
        .build()) { mb =>
        import mb._
        val scVar = `var`(classOf[SparkContext].asType, thisVar.nextLocal)
        val hadoopConfVar = `var`(classOf[Broadcast[Configuration]].asType, scVar.nextLocal)
        val broadcastsVar = `var`(classOf[Map[BroadcastId, Future[Broadcast[_]]]].asType, hadoopConfVar.nextLocal)

        thisVar.push().invokeInit(
          superType,
          scVar.push(),
          hadoopConfVar.push(),
          broadcastsVar.push(),
          getStatic(ClassTag.getClass.asType, "MODULE$", ClassTag.getClass.asType)
            .invokeV("apply", classOf[ClassTag[_]].asType, ldc(keyType).asType(classOf[Class[_]].asType)),
          getStatic(ClassTag.getClass.asType, "MODULE$", ClassTag.getClass.asType)
            .invokeV("apply", classOf[ClassTag[_]].asType, ldc(valueType).asType(classOf[Class[_]].asType)),
          getStatic(ClassTag.getClass.asType, "MODULE$", ClassTag.getClass.asType)
            .invokeV("apply", classOf[ClassTag[_]].asType, ldc(inputFormatType).asType(classOf[Class[_]].asType)))
      }
  }
}

object InputDriverClassBuilder {

  private[this] val curId: AtomicLong = new AtomicLong(0L)

  def nextId: Long = curId.getAndIncrement
}
