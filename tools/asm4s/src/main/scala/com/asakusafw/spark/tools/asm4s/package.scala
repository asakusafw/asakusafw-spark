/*
 * Copyright 2011-2019 Asakusa Framework Team.
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

import scala.collection.GenTraversable
import scala.collection.generic.Growable
import scala.collection.mutable
import scala.reflect.{ AnyValManifest, ClassTag, NameTransformer }

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

  def classTag(t: Type)(implicit mb: MethodBuilder): Stack = {
    pushObject(ClassTag)
      .invokeV("apply", classOf[ClassTag[_]].asType, ldc(t).asType(classOf[Class[_]].asType))
  }

  def manifest(t: Type)(implicit mb: MethodBuilder): Stack = {
    t.getSort() match {
      case Type.BOOLEAN =>
        pushObject(Manifest)
          .invokeV("Boolean", classOf[AnyValManifest[Boolean]].asType)
          .asType(classOf[Manifest[Boolean]].asType)
      case Type.CHAR =>
        pushObject(Manifest)
          .invokeV("Char", classOf[AnyValManifest[Char]].asType)
          .asType(classOf[Manifest[Char]].asType)
      case Type.BYTE =>
        pushObject(Manifest)
          .invokeV("Byte", classOf[AnyValManifest[Byte]].asType)
          .asType(classOf[Manifest[Byte]].asType)
      case Type.SHORT =>
        pushObject(Manifest)
          .invokeV("Short", classOf[AnyValManifest[Short]].asType)
          .asType(classOf[Manifest[Short]].asType)
      case Type.INT =>
        pushObject(Manifest)
          .invokeV("Int", classOf[AnyValManifest[Int]].asType)
          .asType(classOf[Manifest[Int]].asType)
      case Type.LONG =>
        pushObject(Manifest)
          .invokeV("Long", classOf[AnyValManifest[Long]].asType)
          .asType(classOf[Manifest[Long]].asType)
      case Type.FLOAT =>
        pushObject(Manifest)
          .invokeV("Float", classOf[AnyValManifest[Float]].asType)
          .asType(classOf[Manifest[Float]].asType)
      case Type.DOUBLE =>
        pushObject(Manifest)
          .invokeV("Double", classOf[AnyValManifest[Double]].asType)
          .asType(classOf[Manifest[Double]].asType)
      case _ =>
        pushObject(Manifest)
          .invokeV("classType", classOf[Manifest[_]].asType,
            ldc(t).asType(classOf[Class[_]].asType))
    }
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
      (if (values.isEmpty) {
        pushObject(if (indexed) IndexedSeq else Seq)
          .invokeV("empty", classOf[GenTraversable[_]].asType)
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
      })
        .cast(if (indexed) classOf[IndexedSeq[_]].asType else classOf[Seq[_]].asType)
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

  // scalastyle:off

  /*
  for (i <- 2 to 22) {
    println(
      s"""
      |  def tuple${i}(${(1 to i).map(j => s"_${j}: => Stack").mkString(", ")})(implicit mb: MethodBuilder): Stack = {
      |    pushObject(Tuple${i})
      |      .invokeV(
      |        "apply",
      |        classOf[(${(1 to i).map(_ => "_").mkString(", ")})].asType,
      |        ${(1 to i).map(j => s"_${j}.asType(classOf[AnyRef].asType)").mkString(",\n        ")})
      |  }""".stripMargin)
  }
  */

  def tuple2(_1: => Stack, _2: => Stack)(implicit mb: MethodBuilder): Stack = {
    pushObject(Tuple2)
      .invokeV(
        "apply",
        classOf[(_, _)].asType,
        _1.asType(classOf[AnyRef].asType),
        _2.asType(classOf[AnyRef].asType))
  }

  def tuple3(_1: => Stack, _2: => Stack, _3: => Stack)(implicit mb: MethodBuilder): Stack = {
    pushObject(Tuple3)
      .invokeV(
        "apply",
        classOf[(_, _, _)].asType,
        _1.asType(classOf[AnyRef].asType),
        _2.asType(classOf[AnyRef].asType),
        _3.asType(classOf[AnyRef].asType))
  }

  def tuple4(_1: => Stack, _2: => Stack, _3: => Stack, _4: => Stack)(implicit mb: MethodBuilder): Stack = {
    pushObject(Tuple4)
      .invokeV(
        "apply",
        classOf[(_, _, _, _)].asType,
        _1.asType(classOf[AnyRef].asType),
        _2.asType(classOf[AnyRef].asType),
        _3.asType(classOf[AnyRef].asType),
        _4.asType(classOf[AnyRef].asType))
  }

  def tuple5(_1: => Stack, _2: => Stack, _3: => Stack, _4: => Stack, _5: => Stack)(implicit mb: MethodBuilder): Stack = {
    pushObject(Tuple5)
      .invokeV(
        "apply",
        classOf[(_, _, _, _, _)].asType,
        _1.asType(classOf[AnyRef].asType),
        _2.asType(classOf[AnyRef].asType),
        _3.asType(classOf[AnyRef].asType),
        _4.asType(classOf[AnyRef].asType),
        _5.asType(classOf[AnyRef].asType))
  }

  def tuple6(_1: => Stack, _2: => Stack, _3: => Stack, _4: => Stack, _5: => Stack, _6: => Stack)(implicit mb: MethodBuilder): Stack = {
    pushObject(Tuple6)
      .invokeV(
        "apply",
        classOf[(_, _, _, _, _, _)].asType,
        _1.asType(classOf[AnyRef].asType),
        _2.asType(classOf[AnyRef].asType),
        _3.asType(classOf[AnyRef].asType),
        _4.asType(classOf[AnyRef].asType),
        _5.asType(classOf[AnyRef].asType),
        _6.asType(classOf[AnyRef].asType))
  }

  def tuple7(_1: => Stack, _2: => Stack, _3: => Stack, _4: => Stack, _5: => Stack, _6: => Stack, _7: => Stack)(implicit mb: MethodBuilder): Stack = {
    pushObject(Tuple7)
      .invokeV(
        "apply",
        classOf[(_, _, _, _, _, _, _)].asType,
        _1.asType(classOf[AnyRef].asType),
        _2.asType(classOf[AnyRef].asType),
        _3.asType(classOf[AnyRef].asType),
        _4.asType(classOf[AnyRef].asType),
        _5.asType(classOf[AnyRef].asType),
        _6.asType(classOf[AnyRef].asType),
        _7.asType(classOf[AnyRef].asType))
  }

  def tuple8(_1: => Stack, _2: => Stack, _3: => Stack, _4: => Stack, _5: => Stack, _6: => Stack, _7: => Stack, _8: => Stack)(implicit mb: MethodBuilder): Stack = {
    pushObject(Tuple8)
      .invokeV(
        "apply",
        classOf[(_, _, _, _, _, _, _, _)].asType,
        _1.asType(classOf[AnyRef].asType),
        _2.asType(classOf[AnyRef].asType),
        _3.asType(classOf[AnyRef].asType),
        _4.asType(classOf[AnyRef].asType),
        _5.asType(classOf[AnyRef].asType),
        _6.asType(classOf[AnyRef].asType),
        _7.asType(classOf[AnyRef].asType),
        _8.asType(classOf[AnyRef].asType))
  }

  def tuple9(_1: => Stack, _2: => Stack, _3: => Stack, _4: => Stack, _5: => Stack, _6: => Stack, _7: => Stack, _8: => Stack, _9: => Stack)(implicit mb: MethodBuilder): Stack = {
    pushObject(Tuple9)
      .invokeV(
        "apply",
        classOf[(_, _, _, _, _, _, _, _, _)].asType,
        _1.asType(classOf[AnyRef].asType),
        _2.asType(classOf[AnyRef].asType),
        _3.asType(classOf[AnyRef].asType),
        _4.asType(classOf[AnyRef].asType),
        _5.asType(classOf[AnyRef].asType),
        _6.asType(classOf[AnyRef].asType),
        _7.asType(classOf[AnyRef].asType),
        _8.asType(classOf[AnyRef].asType),
        _9.asType(classOf[AnyRef].asType))
  }

  def tuple10(_1: => Stack, _2: => Stack, _3: => Stack, _4: => Stack, _5: => Stack, _6: => Stack, _7: => Stack, _8: => Stack, _9: => Stack, _10: => Stack)(implicit mb: MethodBuilder): Stack = {
    pushObject(Tuple10)
      .invokeV(
        "apply",
        classOf[(_, _, _, _, _, _, _, _, _, _)].asType,
        _1.asType(classOf[AnyRef].asType),
        _2.asType(classOf[AnyRef].asType),
        _3.asType(classOf[AnyRef].asType),
        _4.asType(classOf[AnyRef].asType),
        _5.asType(classOf[AnyRef].asType),
        _6.asType(classOf[AnyRef].asType),
        _7.asType(classOf[AnyRef].asType),
        _8.asType(classOf[AnyRef].asType),
        _9.asType(classOf[AnyRef].asType),
        _10.asType(classOf[AnyRef].asType))
  }

  def tuple11(_1: => Stack, _2: => Stack, _3: => Stack, _4: => Stack, _5: => Stack, _6: => Stack, _7: => Stack, _8: => Stack, _9: => Stack, _10: => Stack, _11: => Stack)(implicit mb: MethodBuilder): Stack = {
    pushObject(Tuple11)
      .invokeV(
        "apply",
        classOf[(_, _, _, _, _, _, _, _, _, _, _)].asType,
        _1.asType(classOf[AnyRef].asType),
        _2.asType(classOf[AnyRef].asType),
        _3.asType(classOf[AnyRef].asType),
        _4.asType(classOf[AnyRef].asType),
        _5.asType(classOf[AnyRef].asType),
        _6.asType(classOf[AnyRef].asType),
        _7.asType(classOf[AnyRef].asType),
        _8.asType(classOf[AnyRef].asType),
        _9.asType(classOf[AnyRef].asType),
        _10.asType(classOf[AnyRef].asType),
        _11.asType(classOf[AnyRef].asType))
  }

  def tuple12(_1: => Stack, _2: => Stack, _3: => Stack, _4: => Stack, _5: => Stack, _6: => Stack, _7: => Stack, _8: => Stack, _9: => Stack, _10: => Stack, _11: => Stack, _12: => Stack)(implicit mb: MethodBuilder): Stack = {
    pushObject(Tuple12)
      .invokeV(
        "apply",
        classOf[(_, _, _, _, _, _, _, _, _, _, _, _)].asType,
        _1.asType(classOf[AnyRef].asType),
        _2.asType(classOf[AnyRef].asType),
        _3.asType(classOf[AnyRef].asType),
        _4.asType(classOf[AnyRef].asType),
        _5.asType(classOf[AnyRef].asType),
        _6.asType(classOf[AnyRef].asType),
        _7.asType(classOf[AnyRef].asType),
        _8.asType(classOf[AnyRef].asType),
        _9.asType(classOf[AnyRef].asType),
        _10.asType(classOf[AnyRef].asType),
        _11.asType(classOf[AnyRef].asType),
        _12.asType(classOf[AnyRef].asType))
  }

  def tuple13(_1: => Stack, _2: => Stack, _3: => Stack, _4: => Stack, _5: => Stack, _6: => Stack, _7: => Stack, _8: => Stack, _9: => Stack, _10: => Stack, _11: => Stack, _12: => Stack, _13: => Stack)(implicit mb: MethodBuilder): Stack = {
    pushObject(Tuple13)
      .invokeV(
        "apply",
        classOf[(_, _, _, _, _, _, _, _, _, _, _, _, _)].asType,
        _1.asType(classOf[AnyRef].asType),
        _2.asType(classOf[AnyRef].asType),
        _3.asType(classOf[AnyRef].asType),
        _4.asType(classOf[AnyRef].asType),
        _5.asType(classOf[AnyRef].asType),
        _6.asType(classOf[AnyRef].asType),
        _7.asType(classOf[AnyRef].asType),
        _8.asType(classOf[AnyRef].asType),
        _9.asType(classOf[AnyRef].asType),
        _10.asType(classOf[AnyRef].asType),
        _11.asType(classOf[AnyRef].asType),
        _12.asType(classOf[AnyRef].asType),
        _13.asType(classOf[AnyRef].asType))
  }

  def tuple14(_1: => Stack, _2: => Stack, _3: => Stack, _4: => Stack, _5: => Stack, _6: => Stack, _7: => Stack, _8: => Stack, _9: => Stack, _10: => Stack, _11: => Stack, _12: => Stack, _13: => Stack, _14: => Stack)(implicit mb: MethodBuilder): Stack = {
    pushObject(Tuple14)
      .invokeV(
        "apply",
        classOf[(_, _, _, _, _, _, _, _, _, _, _, _, _, _)].asType,
        _1.asType(classOf[AnyRef].asType),
        _2.asType(classOf[AnyRef].asType),
        _3.asType(classOf[AnyRef].asType),
        _4.asType(classOf[AnyRef].asType),
        _5.asType(classOf[AnyRef].asType),
        _6.asType(classOf[AnyRef].asType),
        _7.asType(classOf[AnyRef].asType),
        _8.asType(classOf[AnyRef].asType),
        _9.asType(classOf[AnyRef].asType),
        _10.asType(classOf[AnyRef].asType),
        _11.asType(classOf[AnyRef].asType),
        _12.asType(classOf[AnyRef].asType),
        _13.asType(classOf[AnyRef].asType),
        _14.asType(classOf[AnyRef].asType))
  }

  def tuple15(_1: => Stack, _2: => Stack, _3: => Stack, _4: => Stack, _5: => Stack, _6: => Stack, _7: => Stack, _8: => Stack, _9: => Stack, _10: => Stack, _11: => Stack, _12: => Stack, _13: => Stack, _14: => Stack, _15: => Stack)(implicit mb: MethodBuilder): Stack = {
    pushObject(Tuple15)
      .invokeV(
        "apply",
        classOf[(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _)].asType,
        _1.asType(classOf[AnyRef].asType),
        _2.asType(classOf[AnyRef].asType),
        _3.asType(classOf[AnyRef].asType),
        _4.asType(classOf[AnyRef].asType),
        _5.asType(classOf[AnyRef].asType),
        _6.asType(classOf[AnyRef].asType),
        _7.asType(classOf[AnyRef].asType),
        _8.asType(classOf[AnyRef].asType),
        _9.asType(classOf[AnyRef].asType),
        _10.asType(classOf[AnyRef].asType),
        _11.asType(classOf[AnyRef].asType),
        _12.asType(classOf[AnyRef].asType),
        _13.asType(classOf[AnyRef].asType),
        _14.asType(classOf[AnyRef].asType),
        _15.asType(classOf[AnyRef].asType))
  }

  def tuple16(_1: => Stack, _2: => Stack, _3: => Stack, _4: => Stack, _5: => Stack, _6: => Stack, _7: => Stack, _8: => Stack, _9: => Stack, _10: => Stack, _11: => Stack, _12: => Stack, _13: => Stack, _14: => Stack, _15: => Stack, _16: => Stack)(implicit mb: MethodBuilder): Stack = {
    pushObject(Tuple16)
      .invokeV(
        "apply",
        classOf[(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _)].asType,
        _1.asType(classOf[AnyRef].asType),
        _2.asType(classOf[AnyRef].asType),
        _3.asType(classOf[AnyRef].asType),
        _4.asType(classOf[AnyRef].asType),
        _5.asType(classOf[AnyRef].asType),
        _6.asType(classOf[AnyRef].asType),
        _7.asType(classOf[AnyRef].asType),
        _8.asType(classOf[AnyRef].asType),
        _9.asType(classOf[AnyRef].asType),
        _10.asType(classOf[AnyRef].asType),
        _11.asType(classOf[AnyRef].asType),
        _12.asType(classOf[AnyRef].asType),
        _13.asType(classOf[AnyRef].asType),
        _14.asType(classOf[AnyRef].asType),
        _15.asType(classOf[AnyRef].asType),
        _16.asType(classOf[AnyRef].asType))
  }

  def tuple17(_1: => Stack, _2: => Stack, _3: => Stack, _4: => Stack, _5: => Stack, _6: => Stack, _7: => Stack, _8: => Stack, _9: => Stack, _10: => Stack, _11: => Stack, _12: => Stack, _13: => Stack, _14: => Stack, _15: => Stack, _16: => Stack, _17: => Stack)(implicit mb: MethodBuilder): Stack = {
    pushObject(Tuple17)
      .invokeV(
        "apply",
        classOf[(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _)].asType,
        _1.asType(classOf[AnyRef].asType),
        _2.asType(classOf[AnyRef].asType),
        _3.asType(classOf[AnyRef].asType),
        _4.asType(classOf[AnyRef].asType),
        _5.asType(classOf[AnyRef].asType),
        _6.asType(classOf[AnyRef].asType),
        _7.asType(classOf[AnyRef].asType),
        _8.asType(classOf[AnyRef].asType),
        _9.asType(classOf[AnyRef].asType),
        _10.asType(classOf[AnyRef].asType),
        _11.asType(classOf[AnyRef].asType),
        _12.asType(classOf[AnyRef].asType),
        _13.asType(classOf[AnyRef].asType),
        _14.asType(classOf[AnyRef].asType),
        _15.asType(classOf[AnyRef].asType),
        _16.asType(classOf[AnyRef].asType),
        _17.asType(classOf[AnyRef].asType))
  }

  def tuple18(_1: => Stack, _2: => Stack, _3: => Stack, _4: => Stack, _5: => Stack, _6: => Stack, _7: => Stack, _8: => Stack, _9: => Stack, _10: => Stack, _11: => Stack, _12: => Stack, _13: => Stack, _14: => Stack, _15: => Stack, _16: => Stack, _17: => Stack, _18: => Stack)(implicit mb: MethodBuilder): Stack = {
    pushObject(Tuple18)
      .invokeV(
        "apply",
        classOf[(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _)].asType,
        _1.asType(classOf[AnyRef].asType),
        _2.asType(classOf[AnyRef].asType),
        _3.asType(classOf[AnyRef].asType),
        _4.asType(classOf[AnyRef].asType),
        _5.asType(classOf[AnyRef].asType),
        _6.asType(classOf[AnyRef].asType),
        _7.asType(classOf[AnyRef].asType),
        _8.asType(classOf[AnyRef].asType),
        _9.asType(classOf[AnyRef].asType),
        _10.asType(classOf[AnyRef].asType),
        _11.asType(classOf[AnyRef].asType),
        _12.asType(classOf[AnyRef].asType),
        _13.asType(classOf[AnyRef].asType),
        _14.asType(classOf[AnyRef].asType),
        _15.asType(classOf[AnyRef].asType),
        _16.asType(classOf[AnyRef].asType),
        _17.asType(classOf[AnyRef].asType),
        _18.asType(classOf[AnyRef].asType))
  }

  def tuple19(_1: => Stack, _2: => Stack, _3: => Stack, _4: => Stack, _5: => Stack, _6: => Stack, _7: => Stack, _8: => Stack, _9: => Stack, _10: => Stack, _11: => Stack, _12: => Stack, _13: => Stack, _14: => Stack, _15: => Stack, _16: => Stack, _17: => Stack, _18: => Stack, _19: => Stack)(implicit mb: MethodBuilder): Stack = {
    pushObject(Tuple19)
      .invokeV(
        "apply",
        classOf[(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _)].asType,
        _1.asType(classOf[AnyRef].asType),
        _2.asType(classOf[AnyRef].asType),
        _3.asType(classOf[AnyRef].asType),
        _4.asType(classOf[AnyRef].asType),
        _5.asType(classOf[AnyRef].asType),
        _6.asType(classOf[AnyRef].asType),
        _7.asType(classOf[AnyRef].asType),
        _8.asType(classOf[AnyRef].asType),
        _9.asType(classOf[AnyRef].asType),
        _10.asType(classOf[AnyRef].asType),
        _11.asType(classOf[AnyRef].asType),
        _12.asType(classOf[AnyRef].asType),
        _13.asType(classOf[AnyRef].asType),
        _14.asType(classOf[AnyRef].asType),
        _15.asType(classOf[AnyRef].asType),
        _16.asType(classOf[AnyRef].asType),
        _17.asType(classOf[AnyRef].asType),
        _18.asType(classOf[AnyRef].asType),
        _19.asType(classOf[AnyRef].asType))
  }

  def tuple20(_1: => Stack, _2: => Stack, _3: => Stack, _4: => Stack, _5: => Stack, _6: => Stack, _7: => Stack, _8: => Stack, _9: => Stack, _10: => Stack, _11: => Stack, _12: => Stack, _13: => Stack, _14: => Stack, _15: => Stack, _16: => Stack, _17: => Stack, _18: => Stack, _19: => Stack, _20: => Stack)(implicit mb: MethodBuilder): Stack = {
    pushObject(Tuple20)
      .invokeV(
        "apply",
        classOf[(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _)].asType,
        _1.asType(classOf[AnyRef].asType),
        _2.asType(classOf[AnyRef].asType),
        _3.asType(classOf[AnyRef].asType),
        _4.asType(classOf[AnyRef].asType),
        _5.asType(classOf[AnyRef].asType),
        _6.asType(classOf[AnyRef].asType),
        _7.asType(classOf[AnyRef].asType),
        _8.asType(classOf[AnyRef].asType),
        _9.asType(classOf[AnyRef].asType),
        _10.asType(classOf[AnyRef].asType),
        _11.asType(classOf[AnyRef].asType),
        _12.asType(classOf[AnyRef].asType),
        _13.asType(classOf[AnyRef].asType),
        _14.asType(classOf[AnyRef].asType),
        _15.asType(classOf[AnyRef].asType),
        _16.asType(classOf[AnyRef].asType),
        _17.asType(classOf[AnyRef].asType),
        _18.asType(classOf[AnyRef].asType),
        _19.asType(classOf[AnyRef].asType),
        _20.asType(classOf[AnyRef].asType))
  }

  def tuple21(_1: => Stack, _2: => Stack, _3: => Stack, _4: => Stack, _5: => Stack, _6: => Stack, _7: => Stack, _8: => Stack, _9: => Stack, _10: => Stack, _11: => Stack, _12: => Stack, _13: => Stack, _14: => Stack, _15: => Stack, _16: => Stack, _17: => Stack, _18: => Stack, _19: => Stack, _20: => Stack, _21: => Stack)(implicit mb: MethodBuilder): Stack = {
    pushObject(Tuple21)
      .invokeV(
        "apply",
        classOf[(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _)].asType,
        _1.asType(classOf[AnyRef].asType),
        _2.asType(classOf[AnyRef].asType),
        _3.asType(classOf[AnyRef].asType),
        _4.asType(classOf[AnyRef].asType),
        _5.asType(classOf[AnyRef].asType),
        _6.asType(classOf[AnyRef].asType),
        _7.asType(classOf[AnyRef].asType),
        _8.asType(classOf[AnyRef].asType),
        _9.asType(classOf[AnyRef].asType),
        _10.asType(classOf[AnyRef].asType),
        _11.asType(classOf[AnyRef].asType),
        _12.asType(classOf[AnyRef].asType),
        _13.asType(classOf[AnyRef].asType),
        _14.asType(classOf[AnyRef].asType),
        _15.asType(classOf[AnyRef].asType),
        _16.asType(classOf[AnyRef].asType),
        _17.asType(classOf[AnyRef].asType),
        _18.asType(classOf[AnyRef].asType),
        _19.asType(classOf[AnyRef].asType),
        _20.asType(classOf[AnyRef].asType),
        _21.asType(classOf[AnyRef].asType))
  }

  def tuple22(_1: => Stack, _2: => Stack, _3: => Stack, _4: => Stack, _5: => Stack, _6: => Stack, _7: => Stack, _8: => Stack, _9: => Stack, _10: => Stack, _11: => Stack, _12: => Stack, _13: => Stack, _14: => Stack, _15: => Stack, _16: => Stack, _17: => Stack, _18: => Stack, _19: => Stack, _20: => Stack, _21: => Stack, _22: => Stack)(implicit mb: MethodBuilder): Stack = {
    pushObject(Tuple22)
      .invokeV(
        "apply",
        classOf[(_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _)].asType,
        _1.asType(classOf[AnyRef].asType),
        _2.asType(classOf[AnyRef].asType),
        _3.asType(classOf[AnyRef].asType),
        _4.asType(classOf[AnyRef].asType),
        _5.asType(classOf[AnyRef].asType),
        _6.asType(classOf[AnyRef].asType),
        _7.asType(classOf[AnyRef].asType),
        _8.asType(classOf[AnyRef].asType),
        _9.asType(classOf[AnyRef].asType),
        _10.asType(classOf[AnyRef].asType),
        _11.asType(classOf[AnyRef].asType),
        _12.asType(classOf[AnyRef].asType),
        _13.asType(classOf[AnyRef].asType),
        _14.asType(classOf[AnyRef].asType),
        _15.asType(classOf[AnyRef].asType),
        _16.asType(classOf[AnyRef].asType),
        _17.asType(classOf[AnyRef].asType),
        _18.asType(classOf[AnyRef].asType),
        _19.asType(classOf[AnyRef].asType),
        _20.asType(classOf[AnyRef].asType),
        _21.asType(classOf[AnyRef].asType),
        _22.asType(classOf[AnyRef].asType))
  }

  // scalastyle:on
}
