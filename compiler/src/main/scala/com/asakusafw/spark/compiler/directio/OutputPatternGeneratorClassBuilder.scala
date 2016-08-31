/*
 * Copyright 2011-2016 Asakusa Framework Team.
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
package directio

import java.util.concurrent.atomic.AtomicLong

import scala.collection.JavaConversions._
import scala.collection.mutable

import org.objectweb.asm.Type
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.lang.compiler.api.reference.DataModelReference
import com.asakusafw.runtime.value.ValueOption
import com.asakusafw.spark.compiler.directio.OutputPatternGeneratorClassBuilder._
import com.asakusafw.spark.runtime.directio.OutputPatternGenerator
import com.asakusafw.spark.runtime.directio.OutputPatternGenerator.Fragment
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._

class OutputPatternGeneratorClassBuilder(
  dataModelRef: DataModelReference)(
    implicit context: CompilerContext)
  extends ClassBuilder(
    Type.getType(
      s"L${GeneratedClassPackageInternalName}/${context.flowId}/directio/OutputPatternGenerator$$${nextId};"), // scalastyle:ignore
    new ClassSignatureBuilder()
      .newSuperclass {
        _.newClassType(classOf[OutputPatternGenerator[_]].asType) {
          _.newTypeArgument(SignatureVisitor.INSTANCEOF, dataModelRef.getDeclaration.asType)
        }
      },
    classOf[OutputPatternGenerator[_]].asType) {

  override def defConstructors(ctorDef: ConstructorDef): Unit = {
    ctorDef.newInit(Seq(classOf[Seq[Fragment]].asType),
      new MethodSignatureBuilder()
        .newParameterType {
          _.newClassType(classOf[Seq[_]].asType) {
            _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[Fragment].asType)
          }
        }
        .newVoidReturnType()) { implicit mb =>
        val thisVar :: fragmentsVar :: _ = mb.argVars
        thisVar.push().invokeInit(superType, fragmentsVar.push)
      }
  }

  override def defMethods(methodDef: MethodDef): Unit = {
    super.defMethods(methodDef)

    val dataModelType = dataModelRef.getDeclaration.asType

    methodDef.newMethod(
      "getProperty",
      classOf[ValueOption[_]].asType,
      Seq(classOf[AnyRef].asType, classOf[String].asType)) { implicit mb =>
        val thisVar :: dataModelVar :: propertyVar :: _ = mb.argVars
        `return`(
          thisVar.push().invokeV(
            "getProperty",
            classOf[ValueOption[_]].asType,
            dataModelVar.push().cast(dataModelType),
            propertyVar.push()))
      }

    methodDef.newMethod(
      "getProperty",
      classOf[ValueOption[_]].asType,
      Seq(dataModelType, classOf[String].asType)) { implicit mb =>
        val thisVar :: dataModelVar :: propertyVar :: _ = mb.argVars
        dataModelRef.getProperties.foreach { propertyRef =>
          propertyVar.push().unlessNotEqual(ldc(propertyRef.getName.toMemberName)) {
            `return`(
              dataModelVar.push()
                .invokeV(propertyRef.getDeclaration.getName, propertyRef.getType.asType))
          }
        }
        `throw`(pushNew0(classOf[AssertionError].asType))
      }
  }
}

object OutputPatternGeneratorClassBuilder {

  private[this] val curIds: mutable.Map[CompilerContext, AtomicLong] =
    mutable.WeakHashMap.empty

  def nextId(implicit context: CompilerContext): Long =
    curIds.getOrElseUpdate(context, new AtomicLong(0L)).getAndIncrement()

  private[this] val cache: mutable.Map[CompilerContext, mutable.Map[Type, Type]] = // scalastyle:ignore
    mutable.WeakHashMap.empty

  def getOrCompile(
    dataModelRef: DataModelReference)(
      implicit context: CompilerContext): Type = {
    cache.getOrElseUpdate(context, mutable.Map.empty).getOrElseUpdate(
      dataModelRef.getDeclaration.asType,
      context.addClass(new OutputPatternGeneratorClassBuilder(dataModelRef)))
  }
}
