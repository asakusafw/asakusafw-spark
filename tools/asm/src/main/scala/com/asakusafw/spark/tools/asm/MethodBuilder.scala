/*
 * Copyright 2011-2018 Asakusa Framework Team.
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
package com.asakusafw.spark.tools.asm

import java.lang.{
  Boolean => JBoolean,
  Character => JChar,
  Byte => JByte,
  Short => JShort,
  Integer => JInt,
  Long => JLong,
  Float => JFloat,
  Double => JDouble,
  Enum => JEnum
}
import java.lang.invoke.CallSite
import java.lang.invoke.MethodHandles
import java.lang.invoke.MethodType
import java.util.Arrays
import java.util.concurrent.atomic.AtomicInteger

import scala.reflect.ClassTag

import org.objectweb.asm.Handle
import org.objectweb.asm.Label
import org.objectweb.asm.MethodVisitor
import org.objectweb.asm.Opcodes._
import org.objectweb.asm.Type

case class MethodBuilder private[asm] (
  thisType: Type, acc: Int, name: String, methodType: Type, argVars: List[MethodBuilder.Var])(
    private[MethodBuilder] val mv: MethodVisitor,
    private[MethodBuilder] val nextLocal: AtomicInteger) {

  def isPublic: Boolean = (acc & ACC_PUBLIC) != 0

  def isProtected: Boolean = (acc & ACC_PROTECTED) != 0

  def isPrivate: Boolean = (acc & ACC_PRIVATE) != 0

  def isStatic: Boolean = (acc & ACC_STATIC) != 0

  def isFinal: Boolean = (acc & ACC_FINAL) != 0
}

object MethodBuilder {

  def `return`(stack: Stack)(implicit mb: MethodBuilder): Unit = { // scalastyle:ignore
    mb.mv.visitInsn(stack.`type`.getOpcode(IRETURN))
  }

  def `return`()(implicit mb: MethodBuilder): Unit = { // scalastyle:ignore
    mb.mv.visitInsn(RETURN)
  }

  def `throw`(stack: Stack)(implicit mb: MethodBuilder): Unit = { // scalastyle:ignore
    mb.mv.visitInsn(ATHROW)
  }

  def pushNew(`type`: Type)(implicit mb: MethodBuilder): Stack = {
    mb.mv.visitTypeInsn(NEW, `type`.getInternalName())
    Stack(`type`)
  }

  def pushNew0(`type`: Type)(implicit mb: MethodBuilder): Stack = {
    val stack = pushNew(`type`)
    stack.dup().invokeInit()
    stack
  }

  def pushNewArray(`type`: Type, size: Int)(implicit mb: MethodBuilder): Stack = {
    pushNewArray(`type`, ldc(size))
  }

  def pushNewArray(`type`: Type, size: Stack)(implicit mb: MethodBuilder): Stack = {
    `type`.getSort() match {
      case Type.BOOLEAN =>
        mb.mv.visitIntInsn(NEWARRAY, T_BOOLEAN)
      case Type.CHAR =>
        mb.mv.visitIntInsn(NEWARRAY, T_CHAR)
      case Type.BYTE =>
        mb.mv.visitIntInsn(NEWARRAY, T_BYTE)
      case Type.SHORT =>
        mb.mv.visitIntInsn(NEWARRAY, T_SHORT)
      case Type.INT =>
        mb.mv.visitIntInsn(NEWARRAY, T_INT)
      case Type.LONG =>
        mb.mv.visitIntInsn(NEWARRAY, T_LONG)
      case Type.FLOAT =>
        mb.mv.visitIntInsn(NEWARRAY, T_FLOAT)
      case Type.DOUBLE =>
        mb.mv.visitIntInsn(NEWARRAY, T_DOUBLE)
      case _ =>
        mb.mv.visitTypeInsn(ANEWARRAY, `type`.getInternalName())
    }
    Stack(Type.getType("[" + `type`.getDescriptor()))
  }

  def pushNewMultiArray(
    `type`: Type,
    d1size: Int,
    sizes: Int*)(
      implicit mb: MethodBuilder): Stack = {
    pushNewMultiArray(`type`, ldc(d1size), sizes.map(ldc(_)): _*)
  }

  def pushNewMultiArray(
    `type`: Type,
    d1size: Stack,
    sizes: Stack*)(
      implicit mb: MethodBuilder): Stack = {
    mb.mv.visitMultiANewArrayInsn(`type`.getInternalName(), sizes.size + 1)
    Stack(Type.getType(("[" * (sizes.size + 1)) + `type`.getDescriptor()))
  }

  def pushNull(`type`: Type)(implicit mb: MethodBuilder): Stack = {
    mb.mv.visitInsn(ACONST_NULL)
    Stack(`type`.boxed)
  }

  def ldc[A <: Any: ClassTag](value: A)(implicit mb: MethodBuilder): Stack = {
    val `class` = implicitly[ClassTag[A]].runtimeClass
    val `type` = Type.getType(`class`)
    `type`.getSort() match {
      case Type.BOOLEAN =>
        ldc(value.asInstanceOf[Boolean])
      case Type.CHAR =>
        ldc(value.asInstanceOf[Char])
      case Type.BYTE =>
        ldc(value.asInstanceOf[Byte])
      case Type.SHORT =>
        ldc(value.asInstanceOf[Short])
      case Type.INT =>
        ldc(value.asInstanceOf[Int])
      case Type.LONG =>
        ldc(value.asInstanceOf[Long])
      case Type.FLOAT =>
        ldc(value.asInstanceOf[Float])
      case Type.DOUBLE =>
        ldc(value.asInstanceOf[Double])
      case _ =>
        if (`class`.isEnum) {
          val name = value.asInstanceOf[JEnum[_]].name
          getStatic(`type`, name, `type`)
        } else {
          mb.mv.visitLdcInsn(value)
          Stack(`type`)
        }
    }
  }

  def ldc(z: Boolean)(implicit mb: MethodBuilder): Stack = {
    if (z) {
      mb.mv.visitInsn(ICONST_1)
    } else {
      mb.mv.visitInsn(ICONST_0)
    }
    Stack(Type.BOOLEAN_TYPE)
  }

  def ldc(c: Char)(implicit mb: MethodBuilder): Stack = {
    if (c <= 5) {
      mb.mv.visitInsn(ICONST_0 + c)
    } else if (c <= Byte.MaxValue) {
      mb.mv.visitIntInsn(BIPUSH, c)
    } else if (c <= Short.MaxValue) {
      mb.mv.visitIntInsn(SIPUSH, c)
    } else {
      mb.mv.visitLdcInsn(c)
    }
    Stack(Type.CHAR_TYPE)
  }

  def ldc(b: Byte)(implicit mb: MethodBuilder): Stack = {
    if (b >= -1 && b <= 5) {
      mb.mv.visitInsn(ICONST_0 + b)
    } else {
      mb.mv.visitIntInsn(BIPUSH, b)
    }
    Stack(Type.BYTE_TYPE)
  }

  def ldc(s: Short)(implicit mb: MethodBuilder): Stack = {
    if (s >= -1 && s <= 5) {
      mb.mv.visitInsn(ICONST_0 + s)
    } else if (s >= Byte.MinValue && s <= Byte.MaxValue) {
      mb.mv.visitIntInsn(BIPUSH, s)
    } else {
      mb.mv.visitIntInsn(SIPUSH, s)
    }
    Stack(Type.SHORT_TYPE)
  }

  def ldc(i: Int)(implicit mb: MethodBuilder): Stack = {
    if (i >= -1 && i <= 5) {
      mb.mv.visitInsn(ICONST_0 + i)
    } else if (i >= Byte.MinValue && i <= Byte.MaxValue) {
      mb.mv.visitIntInsn(BIPUSH, i)
    } else if (i >= Short.MinValue && i <= Short.MaxValue) {
      mb.mv.visitIntInsn(SIPUSH, i)
    } else {
      mb.mv.visitLdcInsn(i)
    }
    Stack(Type.INT_TYPE)
  }

  def ldc(j: Long)(implicit mb: MethodBuilder): Stack = {
    if (j == 0L) {
      mb.mv.visitInsn(LCONST_0)
    } else if (j == 1L) {
      mb.mv.visitInsn(LCONST_1)
    } else {
      mb.mv.visitLdcInsn(j)
    }
    Stack(Type.LONG_TYPE)
  }

  def ldc(f: Float)(implicit mb: MethodBuilder): Stack = {
    if (f == 0.0f) {
      mb.mv.visitInsn(FCONST_0)
    } else if (f == 1.0f) {
      mb.mv.visitInsn(FCONST_1)
    } else if (f == 2.0f) {
      mb.mv.visitInsn(FCONST_2)
    } else {
      mb.mv.visitLdcInsn(f)
    }
    Stack(Type.FLOAT_TYPE)
  }

  def ldc(d: Double)(implicit mb: MethodBuilder): Stack = {
    if (d == 0.0d) {
      mb.mv.visitInsn(DCONST_0)
    } else if (d == 1.0d) {
      mb.mv.visitInsn(DCONST_1)
    } else {
      mb.mv.visitLdcInsn(d)
    }
    Stack(Type.DOUBLE_TYPE)
  }

  def invokeDynamic(
    name: String,
    bootstrapType: Type,
    bootstrapMethod: String,
    bootstrapArguments: Seq[(Class[_], AnyRef)],
    retType: Type,
    arguments: Stack*)(
      implicit mb: MethodBuilder): Stack = {

    val mt = MethodType.methodType(
      classOf[CallSite],
      Array(
        classOf[MethodHandles.Lookup],
        classOf[String],
        classOf[MethodType]) ++ bootstrapArguments.map(_._1))
    val bootstrap = new Handle(
      H_INVOKESTATIC,
      bootstrapType.getInternalName(),
      bootstrapMethod,
      mt.toMethodDescriptorString())
    mb.mv.visitInvokeDynamicInsn(
      name,
      Type.getMethodDescriptor(
        retType,
        arguments.map(_.`type`): _*),
      bootstrap,
      bootstrapArguments.map(_._2): _*)
    Stack(retType)
  }

  def getStatic(
    owner: Type,
    name: String,
    fieldType: Type)(
      implicit mb: MethodBuilder): Stack = {
    mb.mv.visitFieldInsn(GETSTATIC, owner.getInternalName(), name, fieldType.getDescriptor())
    Stack(fieldType)
  }

  def putStatic(
    owner: Type,
    name: String,
    fieldType: Type,
    value: Stack)(implicit mb: MethodBuilder): Unit = {
    mb.mv.visitFieldInsn(PUTSTATIC, owner.getInternalName(), name, fieldType.getDescriptor())
  }

  def invokeStatic(
    owner: Type,
    name: String,
    arguments: Stack*)(implicit mb: MethodBuilder): Unit = {
    invokeStatic(owner, false, name, arguments: _*)
  }

  def invokeStatic(
    owner: Type,
    itf: Boolean,
    name: String,
    arguments: Stack*)(implicit mb: MethodBuilder): Unit = {
    invoke(INVOKESTATIC, owner, itf, name, Type.VOID_TYPE, arguments: _*)
  }

  def invokeStatic(
    owner: Type,
    name: String,
    retType: Type,
    arguments: Stack*)(implicit mb: MethodBuilder): Stack = {
    invokeStatic(owner, false, name, retType, arguments: _*)
  }

  def invokeStatic(
    owner: Type,
    itf: Boolean,
    name: String,
    retType: Type,
    arguments: Stack*)(implicit mb: MethodBuilder): Stack = {
    invoke(INVOKESTATIC, owner, itf, name, retType, arguments: _*).get
  }

  private def invoke(
    opcode: Int,
    owner: Type,
    itf: Boolean,
    name: String,
    retType: Type,
    arguments: Stack*)(implicit mb: MethodBuilder): Option[Stack] = {
    mb.mv.visitMethodInsn(
      opcode,
      owner.boxed.getInternalName(),
      name,
      Type.getMethodDescriptor(retType, arguments.map(_.`type`): _*),
      itf)
    if (retType != Type.VOID_TYPE) {
      Some(Stack(retType))
    } else {
      None
    }
  }

  private def ifelse(
    opcode: Int)(
      `then`: => Stack, `else`: => Stack)(
        implicit mb: MethodBuilder): Stack = {
    val thenLabel = new Label()
    mb.mv.visitJumpInsn(opcode, thenLabel)
    val e = `else`
    val endLabel = new Label()
    mb.mv.visitJumpInsn(GOTO, endLabel)
    mb.mv.visitLabel(thenLabel)
    val t = `then`
    mb.mv.visitLabel(endLabel)
    require(e.`type` == t.`type`)
    t
  }

  private def unless(opcode: Int)(block: => Unit)(implicit mb: MethodBuilder): Unit = {
    val label = new Label()
    mb.mv.visitJumpInsn(opcode, label)
    block
    mb.mv.visitLabel(label)
  }

  def block(b: FlowControl => Unit)(implicit mb: MethodBuilder): Unit = {
    val start = new Label()
    val end = new Label()
    mb.mv.visitLabel(start)
    b(new FlowControl(start, end))
    mb.mv.visitLabel(end)
  }

  def loop(b: FlowControl => Unit)(implicit mb: MethodBuilder): Unit = {
    block { ctrl =>
      b(ctrl)
      ctrl.continue()
    }
  }

  def whileLoop(cond: => Stack)(block: FlowControl => Unit)(implicit mb: MethodBuilder): Unit = {
    loop { ctrl =>
      cond.unlessTrue(ctrl.break())
      block(ctrl)
    }
  }

  def doWhile(block: FlowControl => Unit)(cond: => Stack)(implicit mb: MethodBuilder): Unit = {
    loop { ctrl =>
      block(ctrl)
      cond.unlessTrue(ctrl.break())
    }
  }

  def tryCatch(
    `try`: => Unit, catches: (Type, Stack => Unit)*)(implicit mb: MethodBuilder): Unit = {
    val start = new Label()
    val end = new Label()
    val next = new Label()
    mb.mv.visitLabel(start)
    `try`
    mb.mv.visitJumpInsn(GOTO, next)
    mb.mv.visitLabel(end)
    catches.foreach {
      case (t, c) =>
        val handler = new Label()
        mb.mv.visitLabel(handler)
        c(Stack(t))
        mb.mv.visitJumpInsn(GOTO, next)
        mb.mv.visitTryCatchBlock(start, end, handler, t.getInternalName())
    }
    mb.mv.visitLabel(next)
  }

  class FlowControl private[MethodBuilder] (start: Label, end: Label) {

    def continue()(implicit mb: MethodBuilder): Unit = {
      mb.mv.visitJumpInsn(GOTO, start)
    }

    def break()(implicit mb: MethodBuilder): Unit = {
      mb.mv.visitJumpInsn(GOTO, end)
    }
  }

  sealed abstract class Value(`type`: Type) {

    lazy val size: Int = `type`.getSize()

    lazy val isPrimitive: Boolean = `type`.isPrimitive

    lazy val isBoolean: Boolean = `type`.isBoolean

    lazy val isChar: Boolean = `type`.isChar

    lazy val isInteger: Boolean = `type`.isInteger

    lazy val isLong: Boolean = `type`.isLong

    lazy val isFloat: Boolean = `type`.isFloat

    lazy val isDouble: Boolean = `type`.isDouble

    lazy val isNumber: Boolean = `type`.isNumber

    lazy val isArray: Boolean = `type`.isArray
  }

  case class Stack private[MethodBuilder] (`type`: Type) extends Value(`type`) {

    def store()(implicit mb: MethodBuilder): Var = {
      store(mb.nextLocal.getAndAdd(size))
    }

    def store(local: Int)(implicit mb: MethodBuilder): Var = {
      assert(local < mb.nextLocal.get)
      mb.mv.visitVarInsn(`type`.getOpcode(ISTORE), local)
      Var(`type`, local)
    }

    def getField(name: String, fieldType: Type)(implicit mb: MethodBuilder): Stack = {
      getField(`type`, name, fieldType)
    }

    def getField(owner: Type, name: String, fieldType: Type)(implicit mb: MethodBuilder): Stack = {
      mb.mv.visitFieldInsn(GETFIELD, owner.getInternalName(), name, fieldType.getDescriptor())
      Stack(fieldType)
    }

    def putField(name: String, value: Stack)(implicit mb: MethodBuilder): Unit = {
      putField(`type`, name, value)
    }

    def putField(owner: Type, name: String, value: Stack)(implicit mb: MethodBuilder): Unit = {
      mb.mv.visitFieldInsn(PUTFIELD, owner.getInternalName(), name, value.`type`.getDescriptor())
    }

    def invokeV(
      name: String,
      arguments: Stack*)(implicit mb: MethodBuilder): Unit = {
      invokeV(`type`, name, arguments: _*)
    }

    def invokeV(
      name: String,
      retType: Type,
      arguments: Stack*)(implicit mb: MethodBuilder): Stack = {
      invokeV(`type`, name, retType, arguments: _*)
    }

    def invokeV(
      owner: Type,
      name: String,
      arguments: Stack*)(implicit mb: MethodBuilder): Unit = {
      invoke(INVOKEVIRTUAL, owner, false, name, Type.VOID_TYPE, arguments: _*)
    }

    def invokeV(
      owner: Type,
      name: String,
      retType: Type,
      arguments: Stack*)(implicit mb: MethodBuilder): Stack = {
      invoke(INVOKEVIRTUAL, owner, false, name, retType, arguments: _*).get
    }

    def invokeI(
      name: String,
      arguments: Stack*)(implicit mb: MethodBuilder): Unit = {
      invokeI(`type`, name, arguments: _*)
    }

    def invokeI(
      name: String,
      retType: Type,
      arguments: Stack*)(implicit mb: MethodBuilder): Stack = {
      invokeI(`type`, name, retType, arguments: _*)
    }

    def invokeI(
      owner: Type,
      name: String,
      arguments: Stack*)(implicit mb: MethodBuilder): Unit = {
      invoke(INVOKEINTERFACE, owner, true, name, Type.VOID_TYPE, arguments: _*)
    }

    def invokeI(
      owner: Type,
      name: String,
      retType: Type,
      arguments: Stack*)(implicit mb: MethodBuilder): Stack = {
      invoke(INVOKEINTERFACE, owner, true, name, retType, arguments: _*).get
    }

    def invokeS(
      name: String,
      arguments: Stack*)(implicit mb: MethodBuilder): Unit = {
      invokeS(`type`, false, name, arguments: _*)
    }

    def invokeS(
      itf: Boolean,
      name: String,
      arguments: Stack*)(implicit mb: MethodBuilder): Unit = {
      invokeS(`type`, itf, name, arguments: _*)
    }

    def invokeS(
      name: String,
      retType: Type,
      arguments: Stack*)(implicit mb: MethodBuilder): Stack = {
      invokeS(`type`, false, name, retType, arguments: _*)
    }

    def invokeS(
      itf: Boolean,
      name: String,
      retType: Type,
      arguments: Stack*)(implicit mb: MethodBuilder): Stack = {
      invokeS(`type`, itf, name, retType, arguments: _*)
    }

    def invokeS(
      owner: Type,
      name: String,
      arguments: Stack*)(implicit mb: MethodBuilder): Unit = {
      invokeS(owner, false, name, arguments: _*)
    }

    def invokeS(
      owner: Type,
      itf: Boolean,
      name: String,
      arguments: Stack*)(implicit mb: MethodBuilder): Unit = {
      invoke(INVOKESPECIAL, owner, itf, name, Type.VOID_TYPE, arguments: _*)
    }

    def invokeS(
      owner: Type,
      name: String,
      retType: Type,
      arguments: Stack*)(implicit mb: MethodBuilder): Stack = {
      invokeS(owner, false, name, retType, arguments: _*)
    }

    def invokeS(
      owner: Type,
      itf: Boolean,
      name: String,
      retType: Type,
      arguments: Stack*)(implicit mb: MethodBuilder): Stack = {
      invoke(INVOKESPECIAL, owner, itf, name, retType, arguments: _*).get
    }

    def invokeInit(
      arguments: Stack*)(implicit mb: MethodBuilder): Unit = {
      invokeInit(`type`, arguments: _*)
    }

    def invokeInit(
      owner: Type,
      arguments: Stack*)(implicit mb: MethodBuilder): Unit = {
      invokeS(owner, "<init>", arguments: _*)
    }

    def asType(`type`: Type): Stack = {
      if (this.`type` == `type`) {
        this
      } else {
        Stack(`type`)
      }
    }

    def cast(cast: Type)(implicit mb: MethodBuilder): Stack = {
      assert(!isPrimitive)
      mb.mv.visitTypeInsn(CHECKCAST, cast.getInternalName())
      Stack(cast)
    }

    def isInstanceOf(`type`: Type)(implicit mb: MethodBuilder): Stack = {
      assert(!isPrimitive)
      mb.mv.visitTypeInsn(INSTANCEOF, `type`.getInternalName())
      Stack(Type.BOOLEAN_TYPE)
    }

    def dup()(implicit mb: MethodBuilder): Stack = {
      size match {
        case 1 => mb.mv.visitInsn(DUP)
        case 2 => mb.mv.visitInsn(DUP2)
      }
      this
    }

    def astore(idx: Stack, value: Stack)(implicit mb: MethodBuilder): Unit = {
      mb.mv.visitInsn(Type.getType(`type`.getDescriptor().drop(1)).getOpcode(IASTORE))
    }

    def aload(idx: Stack)(implicit mb: MethodBuilder): Stack = {
      mb.mv.visitInsn(`type`.getOpcode(IALOAD))
      Stack(Type.getType(`type`.getDescriptor().drop(1)))
    }

    def arraylength()(implicit mb: MethodBuilder): Stack = {
      mb.mv.visitInsn(ARRAYLENGTH)
      Stack(Type.INT_TYPE)
    }

    def pop()(implicit mb: MethodBuilder): Unit = {
      size match {
        case 1 => mb.mv.visitInsn(POP)
        case 2 => mb.mv.visitInsn(POP2)
      }
    }

    def swap(other: Stack)(implicit mb: MethodBuilder): Stack = {
      (size, other.size) match {
        case (1, 1) =>
          mb.mv.visitInsn(SWAP)
        case (1, 2) =>
          mb.mv.visitInsn(DUP2_X1)
          other.pop()
        case (2, 1) =>
          mb.mv.visitInsn(DUP_X2)
          other.pop()
        case (2, 2) =>
          mb.mv.visitInsn(DUP2_X2)
          other.pop()
      }
      this
    }

    def box()(implicit mb: MethodBuilder): Stack = {
      if (isPrimitive) {
        invokeStatic(`type`, "valueOf", `type`.boxed, this)
      } else {
        this
      }
    }

    def unbox()(implicit mb: MethodBuilder): Stack = {
      `type`.getClassName() match {
        case "java.lang.Boolean" => invokeV("booleanValue", Type.BOOLEAN_TYPE)
        case "java.lang.Character" => invokeV("charValue", Type.CHAR_TYPE)
        case "java.lang.Byte" => invokeV("byteValue", Type.BYTE_TYPE)
        case "java.lang.Short" => invokeV("shortValue", Type.SHORT_TYPE)
        case "java.lang.Integer" => invokeV("intValue", Type.INT_TYPE)
        case "java.lang.Long" => invokeV("longValue", Type.LONG_TYPE)
        case "java.lang.Float" => invokeV("floatValue", Type.FLOAT_TYPE)
        case "java.lang.Double" => invokeV("doubleValue", Type.DOUBLE_TYPE)
        case _ => this
      }
    }

    def and(other: Stack)(implicit mb: MethodBuilder): Stack = {
      assert(isBoolean || isInteger || isLong)
      assert(`type` == other.`type`)
      `type`.getSort() match {
        case Type.BOOLEAN | Type.INT =>
          mb.mv.visitInsn(IAND)
          this
        case Type.BYTE | Type.SHORT =>
          mb.mv.visitInsn(IAND)
          Stack(Type.INT_TYPE)
        case Type.LONG =>
          mb.mv.visitInsn(LAND)
          this
      }
    }

    def or(other: Stack)(implicit mb: MethodBuilder): Stack = {
      assert(isBoolean || isInteger || isLong)
      assert(`type` == other.`type`)
      `type`.getSort() match {
        case Type.BOOLEAN | Type.INT =>
          mb.mv.visitInsn(IOR)
          this
        case Type.BYTE | Type.SHORT =>
          mb.mv.visitInsn(IOR)
          Stack(Type.INT_TYPE)
        case Type.LONG =>
          mb.mv.visitInsn(LOR)
          this
      }
    }

    def xor(other: Stack)(implicit mb: MethodBuilder): Stack = {
      assert(isBoolean || isInteger || isLong)
      assert(`type` == other.`type`)
      `type`.getSort() match {
        case Type.BOOLEAN | Type.INT =>
          mb.mv.visitInsn(IXOR)
          this
        case Type.BYTE | Type.SHORT =>
          mb.mv.visitInsn(IXOR)
          Stack(Type.INT_TYPE)
        case Type.LONG =>
          mb.mv.visitInsn(LXOR)
          this
      }
    }

    def not()(implicit mb: MethodBuilder): Stack = {
      assert(isBoolean || isInteger || isLong)
      `type`.getSort() match {
        case Type.BOOLEAN =>
          xor(ldc(true))
        case Type.BYTE =>
          xor(ldc(-1.toByte))
        case Type.SHORT =>
          xor(ldc(-1.toShort))
        case Type.INT =>
          xor(ldc(-1))
        case Type.LONG =>
          xor(ldc(-1L))
      }
    }

    def shl(other: Stack)(implicit mb: MethodBuilder): Stack = {
      assert(isInteger || isLong)
      assert(other.isInteger)
      `type`.getSort() match {
        case Type.INT =>
          mb.mv.visitInsn(ISHL)
          this
        case Type.BYTE | Type.SHORT =>
          mb.mv.visitInsn(ISHL)
          Stack(Type.INT_TYPE)
        case Type.LONG =>
          mb.mv.visitInsn(LSHL)
          this
      }
    }

    def shr(other: Stack)(implicit mb: MethodBuilder): Stack = {
      assert(isInteger || isLong)
      assert(other.isInteger)
      `type`.getSort() match {
        case Type.INT =>
          mb.mv.visitInsn(ISHR)
          this
        case Type.BYTE | Type.SHORT =>
          mb.mv.visitInsn(ISHR)
          Stack(Type.INT_TYPE)
        case Type.LONG =>
          mb.mv.visitInsn(LSHR)
          this
      }
    }

    def ushr(other: Stack)(implicit mb: MethodBuilder): Stack = {
      assert(isInteger || isLong)
      assert(other.isInteger)
      `type`.getSort() match {
        case Type.INT =>
          mb.mv.visitInsn(IUSHR)
          this
        case Type.BYTE | Type.SHORT =>
          mb.mv.visitInsn(IUSHR)
          Stack(Type.INT_TYPE)
        case Type.LONG =>
          mb.mv.visitInsn(LUSHR)
          this
      }
    }

    def ifTrue(`then`: => Stack, `else`: => Stack)(implicit mb: MethodBuilder): Stack = {
      assert(isBoolean)
      ifelse(IFNE)(`then`, `else`)
    }

    def unlessTrue(block: => Unit)(implicit mb: MethodBuilder): Unit = {
      assert(isBoolean)
      unless(IFNE)(block)
    }

    def isTrue()(implicit mb: MethodBuilder): Stack = {
      assert(isBoolean)
      this
    }

    def ifFalse(`then`: => Stack, `else`: => Stack)(implicit mb: MethodBuilder): Stack = {
      assert(isBoolean)
      ifelse(IFEQ)(`then`, `else`)
    }

    def unlessFalse(block: => Unit)(implicit mb: MethodBuilder): Unit = {
      assert(isBoolean)
      unless(IFEQ)(block)
    }

    def isFalse()(implicit mb: MethodBuilder): Stack = {
      assert(isBoolean)
      not()
    }

    def ifEq0(`then`: => Stack, `else`: => Stack)(implicit mb: MethodBuilder): Stack = {
      assert(isPrimitive)
      if (isBoolean || isChar || isInteger) {
        ifelse(IFEQ)(`then`, `else`)
      } else {
        ifEq(
          if (isLong) ldc(0L) else if (isFloat) ldc(0.0f) else ldc(0.0d))(`then`, `else`)
      }
    }

    def isEq0()(implicit mb: MethodBuilder): Stack = {
      ifEq0(ldc(true), ldc(false))
    }

    def unlessEq0(block: => Unit)(implicit mb: MethodBuilder): Unit = {
      assert(isPrimitive)
      if (isBoolean || isChar || isInteger) {
        unless(IFEQ)(block)
      } else {
        unlessEq(
          if (isLong) ldc(0L) else if (isFloat) ldc(0.0f) else ldc(0.0d))(block)
      }
    }

    def ifNe0(`then`: => Stack, `else`: => Stack)(implicit mb: MethodBuilder): Stack = {
      assert(isPrimitive)
      if (isBoolean || isChar || isInteger) {
        ifelse(IFNE)(`then`, `else`)
      } else {
        ifNe(
          if (isLong) ldc(0L) else if (isFloat) ldc(0.0f) else ldc(0.0d))(`then`, `else`)
      }
    }

    def isNe0()(implicit mb: MethodBuilder): Stack = {
      ifNe0(ldc(true), ldc(false))
    }

    def unlessNe0(block: => Unit)(implicit mb: MethodBuilder): Unit = {
      assert(isPrimitive)
      if (isBoolean || isChar || isInteger) {
        unless(IFNE)(block)
      } else {
        unlessNe(
          if (isLong) ldc(0L) else if (isFloat) ldc(0.0f) else ldc(0.0d))(block)
      }
    }

    def ifLe0(`then`: => Stack, `else`: => Stack)(implicit mb: MethodBuilder): Stack = {
      assert(isPrimitive)
      if (isBoolean || isChar || isInteger) {
        ifelse(IFLE)(`then`, `else`)
      } else {
        ifLessThanOrEqual(
          if (isLong) ldc(0L) else if (isFloat) ldc(0.0f) else ldc(0.0d))(`then`, `else`)
      }
    }

    def isLe0()(implicit mb: MethodBuilder): Stack = {
      ifLe0(ldc(true), ldc(false))
    }

    def unlessLe0(block: => Unit)(implicit mb: MethodBuilder): Unit = {
      assert(isPrimitive)
      if (isBoolean || isChar || isInteger) {
        unless(IFLE)(block)
      } else {
        unlessLessThanOrEqual(
          if (isLong) ldc(0L) else if (isFloat) ldc(0.0f) else ldc(0.0d))(block)
      }
    }

    def ifLt0(`then`: => Stack, `else`: => Stack)(implicit mb: MethodBuilder): Stack = {
      assert(isPrimitive)
      if (isBoolean || isChar || isInteger) {
        ifelse(IFLT)(`then`, `else`)
      } else {
        ifLessThan(
          if (isLong) ldc(0L) else if (isFloat) ldc(0.0f) else ldc(0.0d))(`then`, `else`)
      }
    }

    def isLt0()(implicit mb: MethodBuilder): Stack = {
      ifLt0(ldc(true), ldc(false))
    }

    def unlessLt0(block: => Unit)(implicit mb: MethodBuilder): Unit = {
      assert(isPrimitive)
      if (isBoolean || isChar || isInteger) {
        unless(IFLT)(block)
      } else {
        unlessLessThan(
          if (isLong) ldc(0L) else if (isFloat) ldc(0.0f) else ldc(0.0d))(block)
      }
    }

    def ifGe0(`then`: => Stack, `else`: => Stack)(implicit mb: MethodBuilder): Stack = {
      assert(isPrimitive)
      if (isBoolean || isChar || isInteger) {
        ifelse(IFGE)(`then`, `else`)
      } else {
        ifGreaterThanOrEqual(
          if (isLong) ldc(0L) else if (isFloat) ldc(0.0f) else ldc(0.0d))(`then`, `else`)
      }
    }

    def isGe0()(implicit mb: MethodBuilder): Stack = {
      ifGe0(ldc(true), ldc(false))
    }

    def unlessGe0(block: => Unit)(implicit mb: MethodBuilder): Unit = {
      assert(isPrimitive)
      if (isBoolean || isChar || isInteger) {
        unless(IFGE)(block)
      } else {
        unlessGreaterThanOrEqual(
          if (isLong) ldc(0L) else if (isFloat) ldc(0.0f) else ldc(0.0d))(block)
      }
    }

    def ifGt0(`then`: => Stack, `else`: => Stack)(implicit mb: MethodBuilder): Stack = {
      assert(isPrimitive)
      if (isBoolean || isChar || isInteger) {
        ifelse(IFGT)(`then`, `else`)
      } else {
        ifGreaterThan(
          if (isLong) ldc(0L) else if (isFloat) ldc(0.0f) else ldc(0.0d))(`then`, `else`)
      }
    }

    def isGt0()(implicit mb: MethodBuilder): Stack = {
      ifGt0(ldc(true), ldc(false))
    }

    def unlessGt0(block: => Unit)(implicit mb: MethodBuilder): Unit = {
      assert(isPrimitive)
      if (isBoolean || isChar || isInteger) {
        unless(IFGT)(block)
      } else {
        unlessGreaterThan(
          if (isLong) ldc(0L) else if (isFloat) ldc(0.0f) else ldc(0.0d))(block)
      }
    }

    def ifEq(
      other: Stack)(
        `then`: => Stack, `else`: => Stack)(
          implicit mb: MethodBuilder): Stack = {
      `type`.getSort() match {
        case Type.BOOLEAN | Type.CHAR | Type.BYTE | Type.SHORT | Type.INT =>
          assert(`type` == other.`type`)
          ifelse(IF_ICMPEQ)(`then`, `else`)
        case Type.LONG =>
          assert(`type` == other.`type`)
          mb.mv.visitInsn(LCMP)
          ifelse(IFEQ)(`then`, `else`)
        case Type.FLOAT | Type.DOUBLE =>
          assert(`type` == other.`type`)
          invokeStatic(`type`, "compare", Type.INT_TYPE, this, other)
          ifelse(IFEQ)(`then`, `else`)
        case _ =>
          ifelse(IF_ACMPEQ)(`then`, `else`)
      }
    }

    def isEq(other: Stack)(implicit mb: MethodBuilder): Stack = {
      ifEq(other)(ldc(true), ldc(false))
    }

    def unlessEq(other: Stack)(block: => Unit)(implicit mb: MethodBuilder): Unit = {
      `type`.getSort() match {
        case Type.BOOLEAN | Type.CHAR | Type.BYTE | Type.SHORT | Type.INT =>
          assert(`type` == other.`type`)
          unless(IF_ICMPEQ)(block)
        case Type.LONG =>
          assert(`type` == other.`type`)
          mb.mv.visitInsn(LCMP)
          unless(IFEQ)(block)
        case Type.FLOAT | Type.DOUBLE =>
          assert(`type` == other.`type`)
          invokeStatic(`type`, "compare", Type.INT_TYPE, this, other)
          unless(IFEQ)(block)
        case _ =>
          unless(IF_ACMPEQ)(block)
      }
    }

    def ifNe(
      other: Stack)(
        `then`: => Stack, `else`: => Stack)(
          implicit mb: MethodBuilder): Stack = {
      ifEq(other)(`else`, `then`)
    }

    def isNe(other: Stack)(implicit mb: MethodBuilder): Stack = {
      ifNe(other)(ldc(true), ldc(false))
    }

    def unlessNe(other: Stack)(block: => Unit)(implicit mb: MethodBuilder): Unit = {
      `type`.getSort() match {
        case Type.BOOLEAN | Type.CHAR | Type.BYTE | Type.SHORT | Type.INT =>
          assert(`type` == other.`type`)
          unless(IF_ICMPNE)(block)
        case Type.LONG =>
          assert(`type` == other.`type`)
          mb.mv.visitInsn(LCMP)
          unless(IFNE)(block)
        case Type.FLOAT | Type.DOUBLE =>
          assert(`type` == other.`type`)
          invokeStatic(`type`, "compare", Type.INT_TYPE, this, other)
          unless(IFNE)(block)
        case _ =>
          unless(IF_ACMPNE)(block)
      }
    }

    def ifEqual(
      other: Stack)(
        `then`: => Stack, `else`: => Stack)(
          implicit mb: MethodBuilder): Stack = {
      if (isPrimitive) {
        ifEq(other)(`then`, `else`)
      } else {
        invokeV("equals", Type.BOOLEAN_TYPE, other.asType(classOf[AnyRef].asType))
        ifelse(IFNE)(`then`, `else`)
      }
    }

    def isEqual(other: Stack)(implicit mb: MethodBuilder): Stack = {
      ifEqual(other)(ldc(true), ldc(false))
    }

    def unlessEqual(other: Stack)(block: => Unit)(implicit mb: MethodBuilder): Unit = {
      if (isPrimitive) {
        unlessEq(other)(block)
      } else {
        invokeV("equals", Type.BOOLEAN_TYPE, other.asType(classOf[AnyRef].asType))
        unless(IFNE)(block)
      }
    }

    def ifEqualDeeply(
      other: Stack)(
        `then`: => Stack, `else`: => Stack)(
          implicit mb: MethodBuilder): Stack = {
      if (isPrimitive) {
        ifEq(other)(`then`, `else`)
      } else {
        if (isArray) {
          if (`type`.getDimensions() > 1) {
            invokeStatic(
              classOf[Arrays].asType,
              "deepEquals",
              Type.BOOLEAN_TYPE,
              this.asType(classOf[Array[AnyRef]].asType),
              other.asType(classOf[Array[AnyRef]].asType))
          } else if (`type`.getElementType().getSort() < Type.ARRAY) {
            invokeStatic(
              classOf[Arrays].asType,
              "equals",
              Type.BOOLEAN_TYPE,
              this,
              other)
          } else {
            invokeStatic(
              classOf[Arrays].asType,
              "equals",
              Type.BOOLEAN_TYPE,
              this.asType(classOf[Array[AnyRef]].asType),
              other.asType(classOf[Array[AnyRef]].asType))
          }
        } else {
          invokeV("equals", Type.BOOLEAN_TYPE, other.asType(classOf[AnyRef].asType))
        }
        ifelse(IFNE)(`then`, `else`)
      }
    }

    def isEqualDeeply(other: Stack)(implicit mb: MethodBuilder): Stack = {
      ifEqualDeeply(other)(ldc(true), ldc(false))
    }

    def unlessEqualDeeply(other: Stack)(block: => Unit)(implicit mb: MethodBuilder): Unit = {
      if (isPrimitive) {
        unlessEq(other)(block)
      } else {
        if (isArray) {
          if (`type`.getDimensions() > 1) {
            invokeStatic(
              classOf[Arrays].asType,
              "deepEquals",
              Type.BOOLEAN_TYPE,
              this.asType(classOf[Array[AnyRef]].asType),
              other.asType(classOf[Array[AnyRef]].asType))
          } else if (`type`.getElementType().getSort() < Type.ARRAY) {
            invokeStatic(
              classOf[Arrays].asType,
              "equals",
              Type.BOOLEAN_TYPE,
              this,
              other)
          } else {
            invokeStatic(
              classOf[Arrays].asType,
              "equals",
              Type.BOOLEAN_TYPE,
              this.asType(classOf[Array[AnyRef]].asType),
              other.asType(classOf[Array[AnyRef]].asType))
          }
        } else {
          invokeV("equals", Type.BOOLEAN_TYPE, other.asType(classOf[AnyRef].asType))
        }
        unless(IFNE)(block)
      }
    }

    def ifNotEqual(
      other: Stack)(
        `then`: => Stack, `else`: => Stack)(
          implicit mb: MethodBuilder): Stack = {
      ifEqual(other)(`else`, `then`)
    }

    def isNotEqual(other: Stack)(implicit mb: MethodBuilder): Stack = {
      ifNotEqual(other)(ldc(true), ldc(false))
    }

    def unlessNotEqual(other: Stack)(block: => Unit)(implicit mb: MethodBuilder): Unit = {
      if (isPrimitive) {
        unlessNe(other)(block)
      } else {
        invokeV("equals", Type.BOOLEAN_TYPE, other.asType(classOf[AnyRef].asType))
        unless(IFEQ)(block)
      }
    }

    def ifNotEqualDeeply(
      other: Stack)(
        `then`: => Stack, `else`: => Stack)(
          implicit mb: MethodBuilder): Stack = {
      ifEqualDeeply(other)(`else`, `then`)
    }

    def isNotEqualDeeply(other: Stack)(implicit mb: MethodBuilder): Stack = {
      ifNotEqualDeeply(other)(ldc(true), ldc(false))
    }

    def unlessNotEqualDeeply(other: Stack)(block: => Unit)(implicit mb: MethodBuilder): Unit = {
      if (isPrimitive) {
        unlessNe(other)(block)
      } else {
        if (isArray) {
          if (`type`.getDimensions() > 1) {
            invokeStatic(
              classOf[Arrays].asType,
              "deepEquals",
              Type.BOOLEAN_TYPE,
              this.asType(classOf[Array[AnyRef]].asType),
              other.asType(classOf[Array[AnyRef]].asType))
          } else if (`type`.getElementType().getSort() < Type.ARRAY) {
            invokeStatic(
              classOf[Arrays].asType,
              "equals",
              Type.BOOLEAN_TYPE,
              this,
              other)
          } else {
            invokeStatic(
              classOf[Arrays].asType,
              "equals",
              Type.BOOLEAN_TYPE,
              this.asType(classOf[Array[AnyRef]].asType),
              other.asType(classOf[Array[AnyRef]].asType))
          }
        } else {
          invokeV("equals", Type.BOOLEAN_TYPE, other.asType(classOf[AnyRef].asType))
        }
        unless(IFEQ)(block)
      }
    }

    def ifLessThan(
      other: Stack)(
        `then`: => Stack, `else`: => Stack)(
          implicit mb: MethodBuilder): Stack = {
      assert(isPrimitive)
      assert(`type` == other.`type`)
      `type`.getSort() match {
        case Type.BOOLEAN | Type.CHAR | Type.BYTE | Type.SHORT | Type.INT =>
          ifelse(IF_ICMPLT)(`then`, `else`)
        case Type.LONG =>
          mb.mv.visitInsn(LCMP)
          ifelse(IFLT)(`then`, `else`)
        case Type.FLOAT | Type.DOUBLE =>
          invokeStatic(`type`, "compare", Type.INT_TYPE, this, other)
          ifelse(IFLT)(`then`, `else`)
      }
    }

    def isLessThan(other: Stack)(implicit mb: MethodBuilder): Stack = {
      ifLessThan(other)(ldc(true), ldc(false))
    }

    def unlessLessThan(other: Stack)(block: => Unit)(implicit mb: MethodBuilder): Unit = {
      assert(isPrimitive)
      assert(`type` == other.`type`)
      `type`.getSort() match {
        case Type.BOOLEAN | Type.CHAR | Type.BYTE | Type.SHORT | Type.INT =>
          unless(IF_ICMPLT)(block)
        case Type.LONG =>
          mb.mv.visitInsn(LCMP)
          unless(IFLT)(block)
        case Type.FLOAT | Type.DOUBLE =>
          invokeStatic(`type`, "compare", Type.INT_TYPE, this, other)
          unless(IFLT)(block)
      }
    }

    def ifLessThanOrEqual(
      other: Stack)(
        `then`: => Stack, `else`: => Stack)(
          implicit mb: MethodBuilder): Stack = {
      assert(isPrimitive)
      assert(`type` == other.`type`)
      `type`.getSort() match {
        case Type.BOOLEAN | Type.CHAR | Type.BYTE | Type.SHORT | Type.INT =>
          ifelse(IF_ICMPLE)(`then`, `else`)
        case Type.LONG =>
          mb.mv.visitInsn(LCMP)
          ifelse(IFLE)(`then`, `else`)
        case Type.FLOAT | Type.DOUBLE =>
          invokeStatic(`type`, "compare", Type.INT_TYPE, this, other)
          ifelse(IFLE)(`then`, `else`)
      }
    }

    def isLessThanOrEqual(other: Stack)(implicit mb: MethodBuilder): Stack = {
      ifLessThanOrEqual(other)(ldc(true), ldc(false))
    }

    def unlessLessThanOrEqual(other: Stack)(block: => Unit)(implicit mb: MethodBuilder): Unit = {
      assert(isPrimitive)
      assert(`type` == other.`type`)
      `type`.getSort() match {
        case Type.BOOLEAN | Type.CHAR | Type.BYTE | Type.SHORT | Type.INT =>
          unless(IF_ICMPLE)(block)
        case Type.LONG =>
          mb.mv.visitInsn(LCMP)
          unless(IFLE)(block)
        case Type.FLOAT | Type.DOUBLE =>
          invokeStatic(`type`, "compare", Type.INT_TYPE, this, other)
          unless(IFLE)(block)
      }
    }

    def ifGreaterThan(
      other: Stack)(
        `then`: => Stack, `else`: => Stack)(
          implicit mb: MethodBuilder): Stack = {
      assert(isPrimitive)
      assert(`type` == other.`type`)
      `type`.getSort() match {
        case Type.BOOLEAN | Type.CHAR | Type.BYTE | Type.SHORT | Type.INT =>
          ifelse(IF_ICMPGT)(`then`, `else`)
        case Type.LONG =>
          mb.mv.visitInsn(LCMP)
          ifelse(IFGT)(`then`, `else`)
        case Type.FLOAT | Type.DOUBLE =>
          invokeStatic(`type`, "compare", Type.INT_TYPE, this, other)
          ifelse(IFGT)(`then`, `else`)
      }
    }

    def isGreaterThan(other: Stack)(implicit mb: MethodBuilder): Stack = {
      ifGreaterThan(other)(ldc(true), ldc(false))
    }

    def unlessGreaterThan(other: Stack)(block: => Unit)(implicit mb: MethodBuilder): Unit = {
      assert(isPrimitive)
      assert(`type` == other.`type`)
      `type`.getSort() match {
        case Type.BOOLEAN | Type.CHAR | Type.BYTE | Type.SHORT | Type.INT =>
          unless(IF_ICMPGT)(block)
        case Type.LONG =>
          mb.mv.visitInsn(LCMP)
          unless(IFGT)(block)
        case Type.FLOAT | Type.DOUBLE =>
          invokeStatic(`type`, "compare", Type.INT_TYPE, this, other)
          unless(IFGT)(block)
      }
    }

    def ifGreaterThanOrEqual(
      other: Stack)(
        `then`: => Stack, `else`: => Stack)(
          implicit mb: MethodBuilder): Stack = {
      assert(isPrimitive)
      assert(`type` == other.`type`)
      `type`.getSort() match {
        case Type.BOOLEAN | Type.CHAR | Type.BYTE | Type.SHORT | Type.INT =>
          ifelse(IF_ICMPGE)(`then`, `else`)
        case Type.LONG =>
          mb.mv.visitInsn(LCMP)
          ifelse(IFGE)(`then`, `else`)
        case Type.FLOAT | Type.DOUBLE =>
          invokeStatic(`type`, "compare", Type.INT_TYPE, this, other)
          ifelse(IFGE)(`then`, `else`)
      }
    }

    def isGreaterThanOrEqual(other: Stack)(implicit mb: MethodBuilder): Stack = {
      ifGreaterThanOrEqual(other)(ldc(true), ldc(false))
    }

    def unlessGreaterThanOrEqual(
      other: Stack)(
        block: => Unit)(
          implicit mb: MethodBuilder): Unit = {
      assert(isPrimitive)
      assert(`type` == other.`type`)
      `type`.getSort() match {
        case Type.BOOLEAN | Type.CHAR | Type.BYTE | Type.SHORT | Type.INT =>
          unless(IF_ICMPGE)(block)
        case Type.LONG =>
          mb.mv.visitInsn(LCMP)
          unless(IFGE)(block)
        case Type.FLOAT | Type.DOUBLE =>
          invokeStatic(`type`, "compare", Type.INT_TYPE, this, other)
          unless(IFGE)(block)
      }
    }

    def ifNull(`then`: => Stack, `else`: => Stack)(implicit mb: MethodBuilder): Stack = {
      if (isPrimitive) {
        pop()
        `else`
      } else {
        ifelse(IFNULL)(`then`, `else`)
      }
    }

    def isNull()(implicit mb: MethodBuilder): Stack = {
      ifNull(ldc(true), ldc(false))
    }

    def unlessNull(block: => Unit)(implicit mb: MethodBuilder): Unit = {
      if (isPrimitive) {
        pop()
        block
      } else {
        unless(IFNULL)(block)
      }
    }

    def ifNotNull(`then`: => Stack, `else`: => Stack)(implicit mb: MethodBuilder): Stack = {
      ifNull(`else`, `then`)
    }

    def isNotNull()(implicit mb: MethodBuilder): Stack = {
      ifNotNull(ldc(true), ldc(false))
    }

    def unlessNotNull(block: => Unit)(implicit mb: MethodBuilder): Unit = {
      if (isPrimitive) {
        pop()
      } else {
        unless(IFNONNULL)(block)
      }
    }

    def add(operand: Stack)(implicit mb: MethodBuilder): Stack = {
      assert(isNumber)
      assert(`type` == operand.`type`)
      mb.mv.visitInsn(`type`.getOpcode(IADD))
      this
    }

    def subtract(operand: Stack)(implicit mb: MethodBuilder): Stack = {
      assert(isNumber)
      assert(`type` == operand.`type`)
      mb.mv.visitInsn(`type`.getOpcode(ISUB))
      this
    }

    def multiply(operand: Stack)(implicit mb: MethodBuilder): Stack = {
      assert(isNumber)
      assert(`type` == operand.`type`)
      mb.mv.visitInsn(`type`.getOpcode(IMUL))
      this
    }

    def divide(operand: Stack)(implicit mb: MethodBuilder): Stack = {
      assert(isNumber)
      assert(`type` == operand.`type`)
      mb.mv.visitInsn(`type`.getOpcode(IDIV))
      this
    }

    def remainder(operand: Stack)(implicit mb: MethodBuilder): Stack = {
      assert(isNumber)
      assert(`type` == operand.`type`)
      mb.mv.visitInsn(`type`.getOpcode(IREM))
      this
    }

    def negate()(implicit mb: MethodBuilder): Stack = {
      assert(isNumber)
      mb.mv.visitInsn(`type`.getOpcode(INEG))
      this
    }

    def toChar()(implicit mb: MethodBuilder): Stack = {
      assert(isChar || isNumber)
      `type`.getSort() match {
        case Type.CHAR => this
        case Type.BYTE | Type.SHORT | Type.INT =>
          mb.mv.visitInsn(I2C)
          Stack(Type.CHAR_TYPE)
        case Type.LONG =>
          mb.mv.visitInsn(L2I)
          mb.mv.visitInsn(I2C)
          Stack(Type.CHAR_TYPE)
        case Type.FLOAT =>
          mb.mv.visitInsn(F2I)
          mb.mv.visitInsn(I2C)
          Stack(Type.CHAR_TYPE)
        case Type.DOUBLE =>
          mb.mv.visitInsn(D2I)
          mb.mv.visitInsn(I2C)
          Stack(Type.CHAR_TYPE)
      }
    }

    def toByte()(implicit mb: MethodBuilder): Stack = {
      assert(isChar || isNumber)
      `type`.getSort() match {
        case Type.BYTE => this
        case Type.CHAR | Type.SHORT | Type.INT =>
          mb.mv.visitInsn(I2B)
          Stack(Type.BYTE_TYPE)
        case Type.LONG =>
          mb.mv.visitInsn(L2I)
          mb.mv.visitInsn(I2B)
          Stack(Type.BYTE_TYPE)
        case Type.FLOAT =>
          mb.mv.visitInsn(F2I)
          mb.mv.visitInsn(I2B)
          Stack(Type.BYTE_TYPE)
        case Type.DOUBLE =>
          mb.mv.visitInsn(D2I)
          mb.mv.visitInsn(I2B)
          Stack(Type.BYTE_TYPE)
      }
    }

    def toShort()(implicit mb: MethodBuilder): Stack = {
      assert(isChar || isNumber)
      `type`.getSort() match {
        case Type.SHORT => this
        case Type.CHAR | Type.BYTE | Type.INT =>
          mb.mv.visitInsn(I2S)
          Stack(Type.SHORT_TYPE)
        case Type.LONG =>
          mb.mv.visitInsn(L2I)
          mb.mv.visitInsn(I2S)
          Stack(Type.SHORT_TYPE)
        case Type.FLOAT =>
          mb.mv.visitInsn(F2I)
          mb.mv.visitInsn(I2S)
          Stack(Type.SHORT_TYPE)
        case Type.DOUBLE =>
          mb.mv.visitInsn(D2I)
          mb.mv.visitInsn(I2S)
          Stack(Type.SHORT_TYPE)
      }
    }

    def toInt()(implicit mb: MethodBuilder): Stack = {
      assert(isBoolean || isChar || isNumber)
      `type`.getSort() match {
        case Type.BOOLEAN | Type.CHAR | Type.BYTE | Type.SHORT =>
          Stack(Type.INT_TYPE)
        case Type.INT => this
        case Type.LONG =>
          mb.mv.visitInsn(L2I)
          Stack(Type.INT_TYPE)
        case Type.FLOAT =>
          mb.mv.visitInsn(F2I)
          Stack(Type.INT_TYPE)
        case Type.DOUBLE =>
          mb.mv.visitInsn(D2I)
          Stack(Type.INT_TYPE)
      }
    }

    def toLong()(implicit mb: MethodBuilder): Stack = {
      assert(isChar || isNumber)
      `type`.getSort() match {
        case Type.CHAR | Type.BYTE | Type.SHORT | Type.INT =>
          mb.mv.visitInsn(I2L)
          Stack(Type.LONG_TYPE)
        case Type.LONG =>
          this
        case Type.FLOAT =>
          mb.mv.visitInsn(F2L)
          Stack(Type.LONG_TYPE)
        case Type.DOUBLE =>
          mb.mv.visitInsn(D2L)
          Stack(Type.LONG_TYPE)
      }
    }

    def toFloat()(implicit mb: MethodBuilder): Stack = {
      assert(isChar || isNumber)
      `type`.getSort() match {
        case Type.CHAR | Type.BYTE | Type.SHORT | Type.INT =>
          mb.mv.visitInsn(I2F)
          Stack(Type.FLOAT_TYPE)
        case Type.LONG =>
          mb.mv.visitInsn(L2F)
          Stack(Type.FLOAT_TYPE)
        case Type.FLOAT =>
          this
        case Type.DOUBLE =>
          mb.mv.visitInsn(D2F)
          Stack(Type.FLOAT_TYPE)
      }
    }

    def toDouble()(implicit mb: MethodBuilder): Stack = {
      assert(isChar || isNumber)
      `type`.getSort() match {
        case Type.CHAR | Type.BYTE | Type.SHORT | Type.INT =>
          mb.mv.visitInsn(I2D)
          Stack(Type.DOUBLE_TYPE)
        case Type.LONG =>
          mb.mv.visitInsn(L2D)
          Stack(Type.DOUBLE_TYPE)
        case Type.FLOAT =>
          mb.mv.visitInsn(F2D)
          Stack(Type.DOUBLE_TYPE)
        case Type.DOUBLE =>
          this
      }
    }
  }

  case class Var private[asm] (`type`: Type, local: Int) extends Value(`type`) {

    def push()(implicit mb: MethodBuilder): Stack = {
      mb.mv.visitVarInsn(`type`.getOpcode(ILOAD), local)
      Stack(`type`)
    }

    def inc(increment: Int)(implicit mb: MethodBuilder): Var = {
      assert(isInteger)
      mb.mv.visitIincInsn(local, increment)
      this
    }
  }
}
