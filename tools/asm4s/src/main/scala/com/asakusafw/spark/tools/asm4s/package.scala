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
package com.asakusafw.spark.tools

import scala.collection.generic.Growable
import scala.collection.mutable
import scala.reflect.{ ClassTag, NameTransformer }

import org.objectweb.asm.Type

import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._

package object asm4s {

  def pushObject(obj: Any)(implicit mb: MethodBuilder): Stack = {
    getStatic(obj.getClass.asType, "MODULE$", obj.getClass.asType)
  }

  def option(value: => Stack)(implicit mb: MethodBuilder): Stack = {
    pushObject(Option)
      .invokeV("apply", classOf[Option[_]].asType, value.asType(classOf[AnyRef].asType))
  }

  def tuple2(_1: => Stack, _2: => Stack)(implicit mb: MethodBuilder): Stack = {
    pushObject(Tuple2)
      .invokeV(
        "apply",
        classOf[(_, _)].asType,
        _1.asType(classOf[AnyRef].asType),
        _2.asType(classOf[AnyRef].asType))
  }

  def classTag(t: Type)(implicit mb: MethodBuilder): Stack = {
    pushObject(ClassTag)
      .invokeV("apply", classOf[ClassTag[_]].asType, ldc(t).asType(classOf[Class[_]].asType))
  }

  def buildArray(t: Type)(block: ArrayBuilder => Unit)(implicit mb: MethodBuilder): Stack = {
    val builder = new ArrayBuilder(t)
    block(builder)
    builder.result
  }

  def buildSeq(block: SeqBuilder => Unit)(implicit mb: MethodBuilder): Stack = {
    val builder = new SeqBuilder(indexed = false)
    block(builder)
    builder.result
  }

  def buildIndexedSeq(block: SeqBuilder => Unit)(implicit mb: MethodBuilder): Stack = {
    val builder = new SeqBuilder(indexed = true)
    block(builder)
    builder.result
  }

  def buildMap(block: MapBuilder => Unit)(implicit mb: MethodBuilder): Stack = {
    val builder = new MapBuilder()
    block(builder)
    builder.result
  }

  def buildSet(block: SetBuilder => Unit)(implicit mb: MethodBuilder): Stack = {
    val builder = new SetBuilder()
    block(builder)
    builder.result
  }

  def applySeq(seq: => Stack, i: => Stack)(implicit mb: MethodBuilder): Stack = {
    seq.invokeI("apply", classOf[AnyRef].asType, i)
  }

  def applyMap(map: => Stack, key: => Stack)(implicit mb: MethodBuilder): Stack = {
    map.invokeI("apply", classOf[AnyRef].asType, key.asType(classOf[AnyRef].asType))
  }

  def addToMap(map: => Stack, key: => Stack, value: => Stack)(implicit mb: MethodBuilder): Unit = {
    map.invokeI(
      NameTransformer.encode("+="),
      classOf[Growable[_]].asType,
      tuple2(key, value).asType(classOf[AnyRef].asType))
      .pop()
  }

  def addTraversableToMap(
    map: => Stack, traversable: => Stack)(implicit mb: MethodBuilder): Unit = {
    map.invokeI(
      NameTransformer.encode("++="),
      classOf[Growable[_]].asType,
      traversable.asType(classOf[TraversableOnce[_]].asType))
      .pop()
  }

  class ArrayBuilder private[asm4s] (t: Type) {

    private val values = mutable.Buffer.empty[() => Stack]

    def +=(value: => Stack): Unit = { // scalastyle:ignore
      values += { () => value }
    }

    private[asm4s] def result(implicit mb: MethodBuilder): Stack = {
      if (values.isEmpty) {
        pushEmptyArray()
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

    private def pushEmptyArray()(implicit mb: MethodBuilder): Stack = {
      t.getSort() match {
        case Type.BOOLEAN =>
          pushObject(Array).invokeV("emptyBooleanArray", classOf[Array[Boolean]].asType)
        case Type.CHAR =>
          pushObject(Array).invokeV("emptyCharArray", classOf[Array[Char]].asType)
        case Type.BYTE =>
          pushObject(Array).invokeV("emptyByteArray", classOf[Array[Byte]].asType)
        case Type.SHORT =>
          pushObject(Array).invokeV("emptyShortArray", classOf[Array[Short]].asType)
        case Type.INT =>
          pushObject(Array).invokeV("emptyIntArray", classOf[Array[Int]].asType)
        case Type.LONG =>
          pushObject(Array).invokeV("emptyLongArray", classOf[Array[Long]].asType)
        case Type.FLOAT =>
          pushObject(Array).invokeV("emptyFloatArray", classOf[Array[Float]].asType)
        case Type.DOUBLE =>
          pushObject(Array).invokeV("emptyDoubleArray", classOf[Array[Double]].asType)
        case _ =>
          pushNewArray(t, 0)
      }
    }
  }

  class SeqBuilder private[asm4s] (indexed: Boolean) {

    private val values = mutable.Buffer.empty[() => Stack]

    def +=(value: => Stack): Unit = { // scalastyle:ignore
      values += { () => value }
    }

    private[asm4s] def result(implicit mb: MethodBuilder): Stack = {
      if (values.isEmpty) {
        pushObject(Seq).invokeV("empty",
          if (indexed) classOf[IndexedSeq[_]].asType else classOf[Seq[_]].asType)
      } else {
        val builder = (if (indexed) pushObject(IndexedSeq) else pushObject(Seq))
          .invokeV("newBuilder", classOf[mutable.Builder[_, _]].asType)
        for {
          value <- values
        } {
          builder.invokeI(
            NameTransformer.encode("+="),
            classOf[mutable.Builder[_, _]].asType,
            value().asType(classOf[AnyRef].asType))
        }
        builder.invokeI("result", classOf[AnyRef].asType)
          .cast(if (indexed) classOf[IndexedSeq[_]].asType else classOf[Seq[_]].asType)
      }
    }
  }

  class MapBuilder private[asm4s] {

    private val values = mutable.Buffer.empty[(() => Stack, () => Stack)]

    def +=(key: => Stack, value: => Stack): Unit = { // scalastyle:ignore
      values += { (() => key, () => value) }
    }

    private[asm4s] def result(implicit mb: MethodBuilder): Stack = {
      if (values.isEmpty) {
        pushObject(Map).invokeV("empty", classOf[Map[_, _]].asType)
      } else {
        val builder = pushObject(Map)
          .invokeV("newBuilder", classOf[mutable.Builder[_, _]].asType)
        for {
          (key, value) <- values
        } {
          builder.invokeI(
            NameTransformer.encode("+="),
            classOf[mutable.Builder[_, _]].asType,
            tuple2(key(), value()).asType(classOf[AnyRef].asType))
        }
        builder.invokeI("result", classOf[AnyRef].asType).cast(classOf[Map[_, _]].asType)
      }
    }
  }

  class SetBuilder private[asm4s] {

    private val values = mutable.Buffer.empty[() => Stack]

    def +=(value: => Stack): Unit = { // scalastyle:ignore
      values += { () => value }
    }

    private[asm4s] def result(implicit mb: MethodBuilder): Stack = {
      if (values.isEmpty) {
        pushObject(Set).invokeV("empty", classOf[Set[_]].asType)
      } else {
        val builder = pushObject(Set)
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
