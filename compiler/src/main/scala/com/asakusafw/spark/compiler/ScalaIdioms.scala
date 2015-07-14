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

import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._

trait ScalaIdioms {

  def pushObject(mb: MethodBuilder)(obj: Any): Stack = {
    import mb._
    getStatic(obj.getClass.asType, "MODULE$", obj.getClass.asType)
  }

  def option(mb: MethodBuilder)(value: => Stack): Stack = {
    import mb._
    pushObject(mb)(Option)
      .invokeV("apply", classOf[Option[_]].asType, value.asType(classOf[AnyRef].asType))
  }

  def tuple2(mb: MethodBuilder)(_1: => Stack, _2: => Stack): Stack = {
    import mb._
    pushObject(mb)(Tuple2)
      .invokeV(
        "apply",
        classOf[(_, _)].asType,
        _1.asType(classOf[AnyRef].asType),
        _2.asType(classOf[AnyRef].asType))
  }
}
