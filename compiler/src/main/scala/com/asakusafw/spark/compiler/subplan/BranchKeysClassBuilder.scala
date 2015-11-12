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

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable

import org.objectweb.asm.Type

import com.asakusafw.lang.compiler.model.graph.MarkerOperator
import com.asakusafw.spark.compiler.util.ScalaIdioms._
import com.asakusafw.spark.runtime.rdd.BranchKey
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._

trait BranchKeys {
  this: BranchKeysClassBuilder =>

  def getField(mb: MethodBuilder, marker: MarkerOperator): Stack = {
    import mb._ // scalastyle:ignore
    getStatic(thisType, getField(marker.getSerialNumber), classOf[BranchKey].asType)
  }
}

class BranchKeysClassBuilder(flowId: String)
  extends ClassBuilder(
    Type.getType(s"L${GeneratedClassPackageInternalName}/${flowId}/driver/BranchKeys;"),
    classOf[AnyRef].asType)
  with BranchKeys {

  private[this] val branchKeys: mutable.Map[Long, Int] = mutable.Map.empty

  private[this] val curId: AtomicInteger = new AtomicInteger(0)

  private[this] def field(id: Long): String = s"BRANCH_${id}"

  def getField(sn: Long): String = field(branchKeys.getOrElseUpdate(sn, curId.getAndIncrement))

  override def defFields(fieldDef: FieldDef): Unit = {
    branchKeys.values.toSeq.sorted.foreach { id =>
      fieldDef.newStaticFinalField(field(id), classOf[BranchKey].asType)
    }
  }

  override def defConstructors(ctorDef: ConstructorDef): Unit = {
    ctorDef.newStaticInit { mb =>
      import mb._ // scalastyle:ignore
      branchKeys.values.toSeq.sorted.foreach { id =>
        putStatic(thisType, field(id), classOf[BranchKey].asType,
          pushObject(mb)(BranchKey)
            .invokeV("apply", classOf[BranchKey].asType, ldc(id)))
      }
    }
  }

  override def defMethods(methodDef: MethodDef): Unit = {
    super.defMethods(methodDef)

    methodDef.newStaticMethod("valueOf", classOf[BranchKey].asType, Seq(Type.INT_TYPE)) { mb =>
      import mb._ // scalastyle:ignore
      val idVar = `var`(Type.INT_TYPE, 0)

      def bs(min: Int, max: Int): Unit = {
        if (min < max) {
          val id = (max - min) / 2 + min
          idVar.push().unlessNe(ldc(id)) {
            `return`(getStatic(thisType, field(id), classOf[BranchKey].asType))
          }
          if (id + 1 < max) {
            idVar.push().unlessLessThanOrEqual(ldc(id)) {
              bs(id + 1, max)
            }
          }
          bs(min, id)
        }
      }
      bs(0, curId.get)

      `throw` {
        val error = pushNew(classOf[IllegalArgumentException].asType)
        error.dup().invokeInit({
          val builder = pushNew0(classOf[StringBuilder].asType)
          builder.invokeV("append", classOf[StringBuilder].asType,
            ldc("No BranchKey constant for value ["))
          builder.invokeV("append", classOf[StringBuilder].asType, idVar.push())
          builder.invokeV("append", classOf[StringBuilder].asType, ldc("]."))
          builder.invokeV("toString", classOf[String].asType)
        })
        error
      }
    }
  }
}
