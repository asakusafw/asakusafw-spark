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

import scala.collection.mutable
import scala.concurrent.Future

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.objectweb.asm.Type
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.lang.compiler.model.graph.ExternalOutput
import com.asakusafw.spark.compiler.spi.SubPlanCompiler
import com.asakusafw.spark.compiler.subplan.OutputDriverClassBuilder._
import com.asakusafw.spark.runtime.driver.OutputDriver
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm4s._

class OutputDriverClassBuilder(
  val operator: ExternalOutput)(
    val label: String)(
      implicit context: SubPlanCompiler.Context)
  extends ClassBuilder(
    Type.getType(
      s"L${GeneratedClassPackageInternalName}/${context.flowId}/driver/OutputDriver$$${nextId};"),
    new ClassSignatureBuilder()
      .newSuperclass {
        _.newClassType(classOf[OutputDriver[_]].asType) {
          _.newTypeArgument(SignatureVisitor.INSTANCEOF, operator.getDataType.asType)
        }
      }
      .build(),
    classOf[OutputDriver[_]].asType)
  with LabelField {

  override def defConstructors(ctorDef: ConstructorDef): Unit = {
    ctorDef.newInit(Seq(
      classOf[SparkContext].asType,
      classOf[Broadcast[Configuration]].asType,
      classOf[Seq[Future[RDD[(_, _)]]]].asType,
      classOf[mutable.Set[Future[Unit]]].asType),
      new MethodSignatureBuilder()
        .newParameterType(classOf[SparkContext].asType)
        .newParameterType {
          _.newClassType(classOf[Broadcast[_]].asType) {
            _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[Configuration].asType)
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
                        _.newTypeArgument()
                          .newTypeArgument(
                            SignatureVisitor.INSTANCEOF,
                            operator.getDataType.asType)
                      }
                    }
                  }
                }
              }
            }
          }
        }
        .newParameterType {
          _.newClassType(classOf[mutable.Set[_]].asType) {
            _.newTypeArgument(SignatureVisitor.INSTANCEOF) {
              _.newClassType(classOf[Future[_]].asType) {
                _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[Unit].asType)
              }
            }
          }
        }
        .newVoidReturnType()
        .build()) { implicit mb =>
        import mb._ // scalastyle:ignore
        val scVar =
          `var`(classOf[SparkContext].asType, thisVar.nextLocal)
        val hadoopConfVar =
          `var`(classOf[Broadcast[Configuration]].asType, scVar.nextLocal)
        val prevsVar =
          `var`(classOf[Seq[Future[RDD[(_, _)]]]].asType, hadoopConfVar.nextLocal)
        val terminatorsVar =
          `var`(classOf[mutable.Set[Future[Unit]]].asType, prevsVar.nextLocal)

        thisVar.push().invokeInit(
          superType,
          scVar.push(),
          hadoopConfVar.push(),
          prevsVar.push(),
          terminatorsVar.push(),
          classTag(operator.getDataType.asType))
      }
  }

  override def defMethods(methodDef: MethodDef): Unit = {
    super.defMethods(methodDef)

    methodDef.newMethod("path", classOf[String].asType, Seq.empty) { mb =>
      import mb._ // scalastyle:ignore
      `return`(ldc(context.options.getRuntimeWorkingPath(operator.getName)))
    }
  }
}

object OutputDriverClassBuilder {

  private[this] val curId: AtomicLong = new AtomicLong(0L)

  def nextId: Long = curId.getAndIncrement
}
