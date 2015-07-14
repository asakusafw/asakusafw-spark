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

import scala.collection.mutable
import scala.reflect.NameTransformer

import org.objectweb.asm.Type

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

  def buildArray(mb: MethodBuilder, t: Type)(block: ScalaIdioms.ArrayBuilder => Unit): Stack = {
    val builder = new ScalaIdioms.ArrayBuilder(mb, t)
    block(builder)
    builder.result
  }

  def buildSeq(mb: MethodBuilder)(block: ScalaIdioms.SeqBuilder => Unit): Stack = {
    val builder = new ScalaIdioms.SeqBuilder(mb)
    block(builder)
    builder.result
  }

  def buildMap(mb: MethodBuilder)(block: ScalaIdioms.MapBuilder => Unit): Stack = {
    val builder = new ScalaIdioms.MapBuilder(mb)
    block(builder)
    builder.result
  }

  def buildSet(mb: MethodBuilder)(block: ScalaIdioms.SetBuilder => Unit): Stack = {
    val builder = new ScalaIdioms.SetBuilder(mb)
    block(builder)
    builder.result
  }
}

object ScalaIdioms {

  class ArrayBuilder private[ScalaIdioms] (mb: MethodBuilder, t: Type) extends ScalaIdioms {

    private val values = mutable.Buffer.empty[() => Stack]

    def +=(value: => Stack): Unit = {
      values += { () => value }
    }

    private[ScalaIdioms] def result: Stack = {
      import mb._
      if (values.isEmpty) {
        t.getSort() match {
          case Type.BOOLEAN =>
            pushObject(mb)(Array).invokeV("emptyBooleanArray", classOf[Array[Boolean]].asType)
          case Type.CHAR =>
            pushObject(mb)(Array).invokeV("emptyCharArray", classOf[Array[Char]].asType)
          case Type.BYTE =>
            pushObject(mb)(Array).invokeV("emptyByteArray", classOf[Array[Byte]].asType)
          case Type.SHORT =>
            pushObject(mb)(Array).invokeV("emptyShortArray", classOf[Array[Short]].asType)
          case Type.INT =>
            pushObject(mb)(Array).invokeV("emptyIntArray", classOf[Array[Int]].asType)
          case Type.LONG =>
            pushObject(mb)(Array).invokeV("emptyLongArray", classOf[Array[Long]].asType)
          case Type.FLOAT =>
            pushObject(mb)(Array).invokeV("emptyFloatArray", classOf[Array[Float]].asType)
          case Type.DOUBLE =>
            pushObject(mb)(Array).invokeV("emptyDoubleArray", classOf[Array[Double]].asType)
          case _ =>
            pushNewArray(t, 0)
        }
      } else {
        val arr = pushNewArray(t, values.size)
        for {
          (value, i) <- values.zipWithIndex
        } {
          arr.dup().astore(ldc(i), value())
        }
        arr
      }
    }
  }

  class SeqBuilder private[ScalaIdioms] (mb: MethodBuilder) extends ScalaIdioms {

    private val values = mutable.Buffer.empty[() => Stack]

    def +=(value: => Stack): Unit = {
      values += { () => value }
    }

    private[ScalaIdioms] def result: Stack = {
      if (values.isEmpty) {
        pushObject(mb)(Seq).invokeV("empty", classOf[Seq[_]].asType)
      } else {
        val builder = pushObject(mb)(Seq)
          .invokeV("newBuilder", classOf[mutable.Builder[_, _]].asType)
        for {
          value <- values
        } {
          builder.invokeI(
            NameTransformer.encode("+="),
            classOf[mutable.Builder[_, _]].asType,
            value().asType(classOf[AnyRef].asType))
        }
        builder.invokeI("result", classOf[AnyRef].asType).cast(classOf[Seq[_]].asType)
      }
    }
  }

  class MapBuilder private[ScalaIdioms] (mb: MethodBuilder) extends ScalaIdioms {

    private val values = mutable.Buffer.empty[(() => Stack, () => Stack)]

    def +=(key: => Stack, value: => Stack): Unit = {
      values += { (() => key, () => value) }
    }

    private[ScalaIdioms] def result: Stack = {
      if (values.isEmpty) {
        pushObject(mb)(Map).invokeV("empty", classOf[Map[_, _]].asType)
      } else {
        val builder = pushObject(mb)(Map)
          .invokeV("newBuilder", classOf[mutable.Builder[_, _]].asType)
        for {
          (key, value) <- values
        } {
          builder.invokeI(
            NameTransformer.encode("+="),
            classOf[mutable.Builder[_, _]].asType,
            tuple2(mb)(key(), value()).asType(classOf[AnyRef].asType))
        }
        builder.invokeI("result", classOf[AnyRef].asType).cast(classOf[Map[_, _]].asType)
      }
    }
  }

  class SetBuilder private[ScalaIdioms] (mb: MethodBuilder) extends ScalaIdioms {

    private val values = mutable.Buffer.empty[() => Stack]

    def +=(value: => Stack): Unit = {
      values += { () => value }
    }

    private[ScalaIdioms] def result: Stack = {
      if (values.isEmpty) {
        pushObject(mb)(Set).invokeV("empty", classOf[Set[_]].asType)
      } else {
        val builder = pushObject(mb)(Set)
          .invokeV("newBuilder", classOf[mutable.Builder[_, _]].asType)
        for {
          value <- values
        } {
          builder.invokeI(
            NameTransformer.encode("+="),
            classOf[mutable.Builder[_, _]].asType,
            value().asType(classOf[AnyRef].asType))
        }
        builder.invokeI("result", classOf[AnyRef].asType).cast(classOf[Set[_]].asType)
      }
    }
  }
}
