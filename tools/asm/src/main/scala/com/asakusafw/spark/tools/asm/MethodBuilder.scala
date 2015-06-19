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
package com.asakusafw.spark.tools.asm

import java.lang.{
  Boolean => JBoolean,
  Character => JChar,
  Byte => JByte,
  Short => JShort,
  Integer => JInt,
  Long => JLong,
  Float => JFloat,
  Double => JDouble
}
import java.lang.invoke.CallSite
import java.lang.invoke.MethodHandles
import java.lang.invoke.MethodType
import java.util.Arrays

import scala.reflect.ClassTag

import org.objectweb.asm.Handle
import org.objectweb.asm.Label
import org.objectweb.asm.MethodVisitor
import org.objectweb.asm.Opcodes._
import org.objectweb.asm.Type

class MethodBuilder(thisType: Type, private[MethodBuilder] val mv: MethodVisitor) {
  import MethodBuilder._

  val thisVar: Var = {
    new Var(this, thisType, 0)
  }

  def `var`(`type`: Type, local: Int): Var = {
    new Var(this, `type`, local)
  }

  def `return`(stack: Stack): Unit = {
    mv.visitInsn(stack.`type`.getOpcode(IRETURN))
  }

  def `return`(): Unit = {
    mv.visitInsn(RETURN)
  }

  def `throw`(stack: Stack): Unit = {
    mv.visitInsn(ATHROW)
  }

  def pushNew(`type`: Type): Stack = {
    mv.visitTypeInsn(NEW, `type`.getInternalName())
    new Stack(this, `type`)
  }

  def pushNew0(`type`: Type): Stack = {
    val stack = pushNew(`type`)
    stack.dup().invokeInit()
    stack
  }

  def pushNewArray(`type`: Type, size: Int): Stack = {
    pushNewArray(`type`, ldc(size))
  }

  def pushNewArray(`type`: Type, size: Stack): Stack = {
    `type`.getSort() match {
      case Type.BOOLEAN =>
        mv.visitIntInsn(NEWARRAY, T_BOOLEAN)
      case Type.CHAR =>
        mv.visitIntInsn(NEWARRAY, T_CHAR)
      case Type.BYTE =>
        mv.visitIntInsn(NEWARRAY, T_BYTE)
      case Type.SHORT =>
        mv.visitIntInsn(NEWARRAY, T_SHORT)
      case Type.INT =>
        mv.visitIntInsn(NEWARRAY, T_INT)
      case Type.LONG =>
        mv.visitIntInsn(NEWARRAY, T_LONG)
      case Type.FLOAT =>
        mv.visitIntInsn(NEWARRAY, T_FLOAT)
      case Type.DOUBLE =>
        mv.visitIntInsn(NEWARRAY, T_DOUBLE)
      case _ =>
        mv.visitTypeInsn(ANEWARRAY, `type`.getInternalName())
    }
    new Stack(this, Type.getType("[" + `type`.getDescriptor()))
  }

  def pushNewMultiArray(`type`: Type, d1size: Int, sizes: Int*): Stack = {
    pushNewMultiArray(`type`, ldc(d1size), sizes.map(ldc(_)): _*)
  }

  def pushNewMultiArray(`type`: Type, d1size: Stack, sizes: Stack*): Stack = {
    mv.visitMultiANewArrayInsn(`type`.getInternalName(), sizes.size + 1)
    new Stack(this, Type.getType(("[" * (sizes.size + 1)) + `type`.getDescriptor()))
  }

  def pushNull(`type`: Type): Stack = {
    mv.visitInsn(ACONST_NULL)
    new Stack(this, `type`.boxed)
  }

  def ldc[A <: Any: ClassTag](ldc: A): Stack = {
    val `type` = Type.getType(implicitly[ClassTag[A]].runtimeClass)
    `type`.getSort() match {
      case Type.BOOLEAN =>
        if (ldc.asInstanceOf[Boolean]) {
          mv.visitInsn(ICONST_1)
        } else {
          mv.visitInsn(ICONST_0)
        }
      case Type.CHAR =>
        ldc.asInstanceOf[Char].toInt match {
          case i if i <= 5              => mv.visitInsn(ICONST_0 + i)
          case i if i <= Byte.MaxValue  => mv.visitIntInsn(BIPUSH, i)
          case i if i <= Short.MaxValue => mv.visitIntInsn(SIPUSH, i)
          case i                        => mv.visitLdcInsn(i)
        }
      case Type.BYTE =>
        ldc.asInstanceOf[Byte] match {
          case i if i >= -1 && i <= 5 => mv.visitInsn(ICONST_0 + i)
          case i                      => mv.visitIntInsn(BIPUSH, i)
        }
      case Type.SHORT =>
        ldc.asInstanceOf[Short] match {
          case i if i >= -1 && i <= 5 => mv.visitInsn(ICONST_0 + i)
          case i if i >= Byte.MinValue && i <= Byte.MaxValue => mv.visitIntInsn(BIPUSH, i)
          case i => mv.visitIntInsn(SIPUSH, i)
        }
      case Type.INT =>
        ldc.asInstanceOf[Int] match {
          case i if i >= -1 && i <= 5 => mv.visitInsn(ICONST_0 + i)
          case i if i >= Byte.MinValue && i <= Byte.MaxValue => mv.visitIntInsn(BIPUSH, i)
          case i if i >= Short.MinValue && i <= Short.MaxValue => mv.visitIntInsn(SIPUSH, i)
          case i => mv.visitLdcInsn(i)
        }
      case Type.LONG =>
        ldc.asInstanceOf[Long] match {
          case 0L => mv.visitInsn(LCONST_0)
          case 1L => mv.visitInsn(LCONST_1)
          case l  => mv.visitLdcInsn(l)
        }
      case Type.FLOAT =>
        ldc.asInstanceOf[Float] match {
          case 0.0f => mv.visitInsn(FCONST_0)
          case 1.0f => mv.visitInsn(FCONST_1)
          case 2.0f => mv.visitInsn(FCONST_2)
          case f    => mv.visitLdcInsn(f)
        }
      case Type.DOUBLE =>
        ldc.asInstanceOf[Double] match {
          case 0.0d => mv.visitInsn(DCONST_0)
          case 1.0d => mv.visitInsn(DCONST_1)
          case d    => mv.visitLdcInsn(d)
        }
      case _ =>
        mv.visitLdcInsn(ldc)
    }
    new Stack(this, `type`)
  }

  def invokeDynamic(
    name: String,
    bootstrapType: Type,
    bootstrapMethod: String,
    bootstrapArguments: Seq[(Class[_], AnyRef)],
    retType: Type,
    arguments: Stack*): Stack = {

    val mt = MethodType.methodType(
      classOf[CallSite],
      Array(classOf[MethodHandles.Lookup], classOf[String], classOf[MethodType]) ++ bootstrapArguments.map(_._1))
    val bootstrap = new Handle(H_INVOKESTATIC, bootstrapType.getInternalName(), bootstrapMethod, mt.toMethodDescriptorString())
    mv.visitInvokeDynamicInsn(
      name,
      Type.getMethodDescriptor(
        retType,
        arguments.map(_.`type`): _*),
      bootstrap,
      bootstrapArguments.map(_._2): _*)
    new Stack(this, retType)
  }

  def getStatic(owner: Type, name: String, fieldType: Type): Stack = {
    mv.visitFieldInsn(GETSTATIC, owner.getInternalName(), name, fieldType.getDescriptor())
    new Stack(this, fieldType)
  }

  def putStatic(owner: Type, name: String, fieldType: Type, value: Stack): Unit = {
    mv.visitFieldInsn(PUTSTATIC, owner.getInternalName(), name, fieldType.getDescriptor())
  }

  def invokeStatic(owner: Type, name: String, arguments: Stack*): Unit = {
    invokeStatic(owner, false, name, arguments: _*)
  }

  def invokeStatic(owner: Type, itf: Boolean, name: String, arguments: Stack*): Unit = {
    invoke(INVOKESTATIC, owner, itf, name, Type.VOID_TYPE, arguments: _*)
  }

  def invokeStatic(owner: Type, name: String, retType: Type, arguments: Stack*): Stack = {
    invokeStatic(owner, false, name, retType, arguments: _*)
  }

  def invokeStatic(owner: Type, itf: Boolean, name: String, retType: Type, arguments: Stack*): Stack = {
    invoke(INVOKESTATIC, owner, itf, name, retType, arguments: _*).get
  }

  private def invoke(opcode: Int, owner: Type, itf: Boolean, name: String, retType: Type, arguments: Stack*): Option[Stack] = {
    mv.visitMethodInsn(
      opcode,
      owner.boxed.getInternalName(),
      name,
      Type.getMethodDescriptor(retType, arguments.map(_.`type`): _*),
      itf)
    if (retType != Type.VOID_TYPE) {
      Some(new Stack(this, retType))
    } else {
      None
    }
  }

  private def ifelse(opcode: Int)(`then`: => Stack, `else`: => Stack): Stack = {
    val thenLabel = new Label()
    mv.visitJumpInsn(opcode, thenLabel)
    val e = `else`
    val endLabel = new Label()
    mv.visitJumpInsn(GOTO, endLabel)
    mv.visitLabel(thenLabel)
    val t = `then`
    mv.visitLabel(endLabel)
    require(e.`type` == t.`type`)
    t
  }

  private def unless(opcode: Int)(block: => Unit): Unit = {
    val label = new Label()
    mv.visitJumpInsn(opcode, label)
    block
    mv.visitLabel(label)
  }

  def block(b: FlowControl => Unit): Unit = {
    val start = new Label()
    val end = new Label()
    mv.visitLabel(start)
    b(new FlowControl(this, start, end))
    mv.visitLabel(end)
  }

  def loop(b: FlowControl => Unit): Unit = {
    block { ctrl =>
      b(ctrl)
      ctrl.continue()
    }
  }

  def whileLoop(cond: => Stack)(block: FlowControl => Unit): Unit = {
    loop { ctrl =>
      cond.unlessTrue(ctrl.break())
      block(ctrl)
    }
  }

  def doWhile(block: FlowControl => Unit)(cond: => Stack): Unit = {
    loop { ctrl =>
      block(ctrl)
      cond.unlessTrue(ctrl.break())
    }
  }

  def tryCatch(`try`: => Unit, catches: (Type, Stack => Unit)*): Unit = {
    val start = new Label()
    val end = new Label()
    val next = new Label()
    mv.visitLabel(start)
    `try`
    mv.visitJumpInsn(GOTO, next)
    mv.visitLabel(end)
    catches.foreach {
      case (t, c) =>
        val handler = new Label()
        mv.visitLabel(handler)
        c(new Stack(this, t))
        mv.visitJumpInsn(GOTO, next)
        mv.visitTryCatchBlock(start, end, handler, t.getInternalName())
    }
    mv.visitLabel(next)
  }
}

object MethodBuilder {

  class FlowControl private[MethodBuilder] (mb: MethodBuilder, start: Label, end: Label) {
    import mb._

    def continue(): Unit = {
      mv.visitJumpInsn(GOTO, start)
    }

    def break(): Unit = {
      mv.visitJumpInsn(GOTO, end)
    }
  }

  sealed abstract class Value(val `type`: Type) {

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

  class Stack private[MethodBuilder] (mb: MethodBuilder, `type`: Type) extends Value(`type`) {
    import mb._

    def store(local: Int): Var = {
      mv.visitVarInsn(`type`.getOpcode(ISTORE), local)
      new Var(mb, `type`, local)
    }

    def getField(name: String, fieldType: Type): Stack = {
      getField(`type`, name, fieldType)
    }

    def getField(owner: Type, name: String, fieldType: Type): Stack = {
      mv.visitFieldInsn(GETFIELD, owner.getInternalName(), name, fieldType.getDescriptor())
      new Stack(mb, fieldType)
    }

    def putField(name: String, fieldType: Type, value: Stack): Unit = {
      putField(`type`, name, fieldType, value)
    }

    def putField(owner: Type, name: String, fieldType: Type, value: Stack): Unit = {
      mv.visitFieldInsn(PUTFIELD, owner.getInternalName(), name, fieldType.getDescriptor())
    }

    def invokeV(name: String, arguments: Stack*): Unit = {
      invokeV(`type`, name, arguments: _*)
    }

    def invokeV(name: String, retType: Type, arguments: Stack*): Stack = {
      invokeV(`type`, name, retType, arguments: _*)
    }

    def invokeV(owner: Type, name: String, arguments: Stack*): Unit = {
      invoke(INVOKEVIRTUAL, owner, false, name, Type.VOID_TYPE, arguments: _*)
    }

    def invokeV(owner: Type, name: String, retType: Type, arguments: Stack*): Stack = {
      invoke(INVOKEVIRTUAL, owner, false, name, retType, arguments: _*).get
    }

    def invokeI(name: String, arguments: Stack*): Unit = {
      invokeI(`type`, name, arguments: _*)
    }

    def invokeI(name: String, retType: Type, arguments: Stack*): Stack = {
      invokeI(`type`, name, retType, arguments: _*)
    }

    def invokeI(owner: Type, name: String, arguments: Stack*): Unit = {
      invoke(INVOKEINTERFACE, owner, true, name, Type.VOID_TYPE, arguments: _*)
    }

    def invokeI(owner: Type, name: String, retType: Type, arguments: Stack*): Stack = {
      invoke(INVOKEINTERFACE, owner, true, name, retType, arguments: _*).get
    }

    def invokeS(name: String, arguments: Stack*): Unit = {
      invokeS(`type`, false, name, arguments: _*)
    }

    def invokeS(itf: Boolean, name: String, arguments: Stack*): Unit = {
      invokeS(`type`, itf, name, arguments: _*)
    }

    def invokeS(name: String, retType: Type, arguments: Stack*): Stack = {
      invokeS(`type`, false, name, retType, arguments: _*)
    }

    def invokeS(itf: Boolean, name: String, retType: Type, arguments: Stack*): Stack = {
      invokeS(`type`, itf, name, retType, arguments: _*)
    }

    def invokeS(owner: Type, name: String, arguments: Stack*): Unit = {
      invokeS(owner, false, name, arguments: _*)
    }

    def invokeS(owner: Type, itf: Boolean, name: String, arguments: Stack*): Unit = {
      invoke(INVOKESPECIAL, owner, itf, name, Type.VOID_TYPE, arguments: _*)
    }

    def invokeS(owner: Type, name: String, retType: Type, arguments: Stack*): Stack = {
      invokeS(owner, false, name, retType, arguments: _*)
    }

    def invokeS(owner: Type, itf: Boolean, name: String, retType: Type, arguments: Stack*): Stack = {
      invoke(INVOKESPECIAL, owner, itf, name, retType, arguments: _*).get
    }

    def invokeInit(arguments: Stack*): Unit = {
      invokeInit(`type`, arguments: _*)
    }

    def invokeInit(owner: Type, arguments: Stack*): Unit = {
      invokeS(owner, "<init>", arguments: _*)
    }

    def asType(`type`: Type): Stack = {
      if (this.`type` == `type`) {
        this
      } else {
        new Stack(mb, `type`)
      }
    }

    def cast(cast: Type): Stack = {
      assert(!isPrimitive)
      mv.visitTypeInsn(CHECKCAST, cast.getInternalName())
      new Stack(mb, cast)
    }

    def isInstanceOf(`type`: Type): Stack = {
      assert(!isPrimitive)
      mv.visitTypeInsn(INSTANCEOF, `type`.getInternalName())
      new Stack(mb, Type.BOOLEAN_TYPE)
    }

    def dup(): Stack = {
      size match {
        case 1 => mv.visitInsn(DUP)
        case 2 => mv.visitInsn(DUP2)
      }
      this
    }

    def astore(idx: Stack, value: Stack): Unit = {
      mv.visitInsn(Type.getType(`type`.getDescriptor().drop(1)).getOpcode(IASTORE))
    }

    def aload(idx: Stack): Stack = {
      mv.visitInsn(`type`.getOpcode(IALOAD))
      new Stack(mb, Type.getType(`type`.getDescriptor().drop(1)))
    }

    def arraylength(): Stack = {
      mv.visitInsn(ARRAYLENGTH)
      new Stack(mb, Type.INT_TYPE)
    }

    def pop(): Unit = {
      size match {
        case 1 => mv.visitInsn(POP)
        case 2 => mv.visitInsn(POP2)
      }
    }

    def swap(other: Stack): Stack = {
      (size, other.size) match {
        case (1, 1) =>
          mv.visitInsn(SWAP)
        case (1, 2) =>
          mv.visitInsn(DUP2_X1)
          other.pop()
        case (2, 1) =>
          mv.visitInsn(DUP_X2)
          other.pop()
        case (2, 2) =>
          mv.visitInsn(DUP2_X2)
          other.pop()
      }
      this
    }

    def box(): Stack = {
      if (isPrimitive) {
        invokeStatic(`type`, "valueOf", `type`.boxed, this)
      } else {
        this
      }
    }

    def unbox(): Stack = {
      `type`.getClassName() match {
        case "java.lang.Boolean"   => invokeV("booleanValue", Type.BOOLEAN_TYPE)
        case "java.lang.Character" => invokeV("charValue", Type.CHAR_TYPE)
        case "java.lang.Byte"      => invokeV("byteValue", Type.BYTE_TYPE)
        case "java.lang.Short"     => invokeV("shortValue", Type.SHORT_TYPE)
        case "java.lang.Integer"   => invokeV("intValue", Type.INT_TYPE)
        case "java.lang.Long"      => invokeV("longValue", Type.LONG_TYPE)
        case "java.lang.Float"     => invokeV("floatValue", Type.FLOAT_TYPE)
        case "java.lang.Double"    => invokeV("doubleValue", Type.DOUBLE_TYPE)
        case _                     => this
      }
    }

    def and(other: Stack): Stack = {
      assert(isBoolean || isInteger || isLong)
      assert(`type` == other.`type`)
      `type`.getSort() match {
        case Type.BOOLEAN | Type.INT =>
          mv.visitInsn(IAND)
          this
        case Type.BYTE | Type.SHORT =>
          mv.visitInsn(IAND)
          new Stack(mb, Type.INT_TYPE)
        case Type.LONG =>
          mv.visitInsn(LAND)
          this
      }
    }

    def or(other: Stack): Stack = {
      assert(isBoolean || isInteger || isLong)
      assert(`type` == other.`type`)
      `type`.getSort() match {
        case Type.BOOLEAN | Type.INT =>
          mv.visitInsn(IOR)
          this
        case Type.BYTE | Type.SHORT =>
          mv.visitInsn(IOR)
          new Stack(mb, Type.INT_TYPE)
        case Type.LONG =>
          mv.visitInsn(LOR)
          this
      }
    }

    def xor(other: Stack): Stack = {
      assert(isBoolean || isInteger || isLong)
      assert(`type` == other.`type`)
      `type`.getSort() match {
        case Type.BOOLEAN | Type.INT =>
          mv.visitInsn(IXOR)
          this
        case Type.BYTE | Type.SHORT =>
          mv.visitInsn(IXOR)
          new Stack(mb, Type.INT_TYPE)
        case Type.LONG =>
          mv.visitInsn(LXOR)
          this
      }
    }

    def not(): Stack = {
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

    def shl(other: Stack): Stack = {
      assert(isInteger || isLong)
      assert(other.isInteger)
      `type`.getSort() match {
        case Type.INT =>
          mv.visitInsn(ISHL)
          this
        case Type.BYTE | Type.SHORT =>
          mv.visitInsn(ISHL)
          new Stack(mb, Type.INT_TYPE)
        case Type.LONG =>
          mv.visitInsn(LSHL)
          this
      }
    }

    def shr(other: Stack): Stack = {
      assert(isInteger || isLong)
      assert(other.isInteger)
      `type`.getSort() match {
        case Type.INT =>
          mv.visitInsn(ISHR)
          this
        case Type.BYTE | Type.SHORT =>
          mv.visitInsn(ISHR)
          new Stack(mb, Type.INT_TYPE)
        case Type.LONG =>
          mv.visitInsn(LSHR)
          this
      }
    }

    def ushr(other: Stack): Stack = {
      assert(isInteger || isLong)
      assert(other.isInteger)
      `type`.getSort() match {
        case Type.INT =>
          mv.visitInsn(IUSHR)
          this
        case Type.BYTE | Type.SHORT =>
          mv.visitInsn(IUSHR)
          new Stack(mb, Type.INT_TYPE)
        case Type.LONG =>
          mv.visitInsn(LUSHR)
          this
      }
    }

    def ifTrue(`then`: => Stack, `else`: => Stack): Stack = {
      assert(isBoolean)
      ifelse(IFNE)(`then`, `else`)
    }

    def unlessTrue(block: => Unit): Unit = {
      assert(isBoolean)
      unless(IFNE)(block)
    }

    def isTrue(): Stack = {
      assert(isBoolean)
      this
    }

    def ifFalse(`then`: => Stack, `else`: => Stack): Stack = {
      assert(isBoolean)
      ifelse(IFEQ)(`then`, `else`)
    }

    def unlessFalse(block: => Unit): Unit = {
      assert(isBoolean)
      unless(IFEQ)(block)
    }

    def isFalse(): Stack = {
      assert(isBoolean)
      not()
    }

    def ifEq0(`then`: => Stack, `else`: => Stack): Stack = {
      assert(isPrimitive)
      if (isBoolean || isChar || isInteger) {
        ifelse(IFEQ)(`then`, `else`)
      } else {
        ifEq(if (isLong) ldc(0L) else if (isFloat) ldc(0.0f) else ldc(0.0d))(`then`, `else`)
      }
    }

    def isEq0(): Stack = {
      ifEq0(ldc(true), ldc(false))
    }

    def unlessEq0(block: => Unit): Unit = {
      assert(isPrimitive)
      if (isBoolean || isChar || isInteger) {
        unless(IFEQ)(block)
      } else {
        unlessEq(if (isLong) ldc(0L) else if (isFloat) ldc(0.0f) else ldc(0.0d))(block)
      }
    }

    def ifNe0(`then`: => Stack, `else`: => Stack): Stack = {
      assert(isPrimitive)
      if (isBoolean || isChar || isInteger) {
        ifelse(IFNE)(`then`, `else`)
      } else {
        ifNe(if (isLong) ldc(0L) else if (isFloat) ldc(0.0f) else ldc(0.0d))(`then`, `else`)
      }
    }

    def isNe0(): Stack = {
      ifNe0(ldc(true), ldc(false))
    }

    def unlessNe0(block: => Unit): Unit = {
      assert(isPrimitive)
      if (isBoolean || isChar || isInteger) {
        unless(IFNE)(block)
      } else {
        unlessNe(if (isLong) ldc(0L) else if (isFloat) ldc(0.0f) else ldc(0.0d))(block)
      }
    }

    def ifLe0(`then`: => Stack, `else`: => Stack): Stack = {
      assert(isPrimitive)
      if (isBoolean || isChar || isInteger) {
        ifelse(IFLE)(`then`, `else`)
      } else {
        ifLessThanOrEqual(if (isLong) ldc(0L) else if (isFloat) ldc(0.0f) else ldc(0.0d))(`then`, `else`)
      }
    }

    def isLe0(): Stack = {
      ifLe0(ldc(true), ldc(false))
    }

    def unlessLe0(block: => Unit): Unit = {
      assert(isPrimitive)
      if (isBoolean || isChar || isInteger) {
        unless(IFLE)(block)
      } else {
        unlessLessThanOrEqual(if (isLong) ldc(0L) else if (isFloat) ldc(0.0f) else ldc(0.0d))(block)
      }
    }

    def ifLt0(`then`: => Stack, `else`: => Stack): Stack = {
      assert(isPrimitive)
      if (isBoolean || isChar || isInteger) {
        ifelse(IFLT)(`then`, `else`)
      } else {
        ifLessThan(if (isLong) ldc(0L) else if (isFloat) ldc(0.0f) else ldc(0.0d))(`then`, `else`)
      }
    }

    def isLt0(): Stack = {
      ifLt0(ldc(true), ldc(false))
    }

    def unlessLt0(block: => Unit): Unit = {
      assert(isPrimitive)
      if (isBoolean || isChar || isInteger) {
        unless(IFLT)(block)
      } else {
        unlessLessThan(if (isLong) ldc(0L) else if (isFloat) ldc(0.0f) else ldc(0.0d))(block)
      }
    }

    def ifGe0(`then`: => Stack, `else`: => Stack): Stack = {
      assert(isPrimitive)
      if (isBoolean || isChar || isInteger) {
        ifelse(IFGE)(`then`, `else`)
      } else {
        ifGreaterThanOrEqual(if (isLong) ldc(0L) else if (isFloat) ldc(0.0f) else ldc(0.0d))(`then`, `else`)
      }
    }

    def isGe0(): Stack = {
      ifGe0(ldc(true), ldc(false))
    }

    def unlessGe0(block: => Unit): Unit = {
      assert(isPrimitive)
      if (isBoolean || isChar || isInteger) {
        unless(IFGE)(block)
      } else {
        unlessGreaterThanOrEqual(if (isLong) ldc(0L) else if (isFloat) ldc(0.0f) else ldc(0.0d))(block)
      }
    }

    def ifGt0(`then`: => Stack, `else`: => Stack): Stack = {
      assert(isPrimitive)
      if (isBoolean || isChar || isInteger) {
        ifelse(IFGT)(`then`, `else`)
      } else {
        ifGreaterThan(if (isLong) ldc(0L) else if (isFloat) ldc(0.0f) else ldc(0.0d))(`then`, `else`)
      }
    }

    def isGt0(): Stack = {
      ifGt0(ldc(true), ldc(false))
    }

    def unlessGt0(block: => Unit): Unit = {
      assert(isPrimitive)
      if (isBoolean || isChar || isInteger) {
        unless(IFGT)(block)
      } else {
        unlessGreaterThan(if (isLong) ldc(0L) else if (isFloat) ldc(0.0f) else ldc(0.0d))(block)
      }
    }

    def ifEq(other: Stack)(`then`: => Stack, `else`: => Stack): Stack = {
      `type`.getSort() match {
        case Type.BOOLEAN | Type.CHAR | Type.BYTE | Type.SHORT | Type.INT =>
          assert(`type` == other.`type`)
          ifelse(IF_ICMPEQ)(`then`, `else`)
        case Type.LONG =>
          assert(`type` == other.`type`)
          mv.visitInsn(LCMP)
          ifelse(IFEQ)(`then`, `else`)
        case Type.FLOAT | Type.DOUBLE =>
          assert(`type` == other.`type`)
          invokeStatic(`type`, "compare", Type.INT_TYPE, this, other)
          ifelse(IFEQ)(`then`, `else`)
        case _ =>
          ifelse(IF_ACMPEQ)(`then`, `else`)
      }
    }

    def isEq(other: Stack): Stack = {
      ifEq(other)(ldc(true), ldc(false))
    }

    def unlessEq(other: Stack)(block: => Unit): Unit = {
      `type`.getSort() match {
        case Type.BOOLEAN | Type.CHAR | Type.BYTE | Type.SHORT | Type.INT =>
          assert(`type` == other.`type`)
          unless(IF_ICMPEQ)(block)
        case Type.LONG =>
          assert(`type` == other.`type`)
          mv.visitInsn(LCMP)
          unless(IFEQ)(block)
        case Type.FLOAT | Type.DOUBLE =>
          assert(`type` == other.`type`)
          invokeStatic(`type`, "compare", Type.INT_TYPE, this, other)
          unless(IFEQ)(block)
        case _ =>
          unless(IF_ACMPEQ)(block)
      }
    }

    def ifNe(other: Stack)(`then`: => Stack, `else`: => Stack): Stack = {
      ifEq(other)(`else`, `then`)
    }

    def isNe(other: Stack): Stack = {
      ifNe(other)(ldc(true), ldc(false))
    }

    def unlessNe(other: Stack)(block: => Unit): Unit = {
      `type`.getSort() match {
        case Type.BOOLEAN | Type.CHAR | Type.BYTE | Type.SHORT | Type.INT =>
          assert(`type` == other.`type`)
          unless(IF_ICMPNE)(block)
        case Type.LONG =>
          assert(`type` == other.`type`)
          mv.visitInsn(LCMP)
          unless(IFNE)(block)
        case Type.FLOAT | Type.DOUBLE =>
          assert(`type` == other.`type`)
          invokeStatic(`type`, "compare", Type.INT_TYPE, this, other)
          unless(IFNE)(block)
        case _ =>
          unless(IF_ACMPNE)(block)
      }
    }

    def ifEqual(other: Stack)(`then`: => Stack, `else`: => Stack): Stack = {
      if (isPrimitive) {
        ifEq(other)(`then`, `else`)
      } else {
        invokeV("equals", Type.BOOLEAN_TYPE, other.asType(classOf[AnyRef].asType))
        ifelse(IFNE)(`then`, `else`)
      }
    }

    def isEqual(other: Stack): Stack = {
      ifEqual(other)(ldc(true), ldc(false))
    }

    def unlessEqual(other: Stack)(block: => Unit): Unit = {
      if (isPrimitive) {
        unlessEq(other)(block)
      } else {
        invokeV("equals", Type.BOOLEAN_TYPE, other.asType(classOf[AnyRef].asType))
        unless(IFNE)(block)
      }
    }

    def ifEqualDeeply(other: Stack)(`then`: => Stack, `else`: => Stack): Stack = {
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

    def isEqualDeeply(other: Stack): Stack = {
      ifEqualDeeply(other)(ldc(true), ldc(false))
    }

    def unlessEqualDeeply(other: Stack)(block: => Unit): Unit = {
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

    def ifNotEqual(other: Stack)(`then`: => Stack, `else`: => Stack): Stack = {
      ifEqual(other)(`else`, `then`)
    }

    def isNotEqual(other: Stack): Stack = {
      ifNotEqual(other)(ldc(true), ldc(false))
    }

    def unlessNotEqual(other: Stack)(block: => Unit): Unit = {
      if (isPrimitive) {
        unlessNe(other)(block)
      } else {
        invokeV("equals", Type.BOOLEAN_TYPE, other.asType(classOf[AnyRef].asType))
        unless(IFEQ)(block)
      }
    }

    def ifNotEqualDeeply(other: Stack)(`then`: => Stack, `else`: => Stack): Stack = {
      ifEqualDeeply(other)(`else`, `then`)
    }

    def isNotEqualDeeply(other: Stack): Stack = {
      ifNotEqualDeeply(other)(ldc(true), ldc(false))
    }

    def unlessNotEqualDeeply(other: Stack)(block: => Unit): Unit = {
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

    def ifLessThan(other: Stack)(`then`: => Stack, `else`: => Stack): Stack = {
      assert(isPrimitive)
      assert(`type` == other.`type`)
      `type`.getSort() match {
        case Type.BOOLEAN | Type.CHAR | Type.BYTE | Type.SHORT | Type.INT =>
          ifelse(IF_ICMPLT)(`then`, `else`)
        case Type.LONG =>
          mv.visitInsn(LCMP)
          ifelse(IFLT)(`then`, `else`)
        case Type.FLOAT | Type.DOUBLE =>
          invokeStatic(`type`, "compare", Type.INT_TYPE, this, other)
          ifelse(IFLT)(`then`, `else`)
      }
    }

    def isLessThan(other: Stack): Stack = {
      ifLessThan(other)(ldc(true), ldc(false))
    }

    def unlessLessThan(other: Stack)(block: => Unit): Unit = {
      assert(isPrimitive)
      assert(`type` == other.`type`)
      `type`.getSort() match {
        case Type.BOOLEAN | Type.CHAR | Type.BYTE | Type.SHORT | Type.INT =>
          unless(IF_ICMPLT)(block)
        case Type.LONG =>
          mv.visitInsn(LCMP)
          unless(IFLT)(block)
        case Type.FLOAT | Type.DOUBLE =>
          invokeStatic(`type`, "compare", Type.INT_TYPE, this, other)
          unless(IFLT)(block)
      }
    }

    def ifLessThanOrEqual(other: Stack)(`then`: => Stack, `else`: => Stack): Stack = {
      assert(isPrimitive)
      assert(`type` == other.`type`)
      `type`.getSort() match {
        case Type.BOOLEAN | Type.CHAR | Type.BYTE | Type.SHORT | Type.INT =>
          ifelse(IF_ICMPLE)(`then`, `else`)
        case Type.LONG =>
          mv.visitInsn(LCMP)
          ifelse(IFLE)(`then`, `else`)
        case Type.FLOAT | Type.DOUBLE =>
          invokeStatic(`type`, "compare", Type.INT_TYPE, this, other)
          ifelse(IFLE)(`then`, `else`)
      }
    }

    def isLessThanOrEqual(other: Stack): Stack = {
      ifLessThanOrEqual(other)(ldc(true), ldc(false))
    }

    def unlessLessThanOrEqual(other: Stack)(block: => Unit): Unit = {
      assert(isPrimitive)
      assert(`type` == other.`type`)
      `type`.getSort() match {
        case Type.BOOLEAN | Type.CHAR | Type.BYTE | Type.SHORT | Type.INT =>
          unless(IF_ICMPLE)(block)
        case Type.LONG =>
          mv.visitInsn(LCMP)
          unless(IFLE)(block)
        case Type.FLOAT | Type.DOUBLE =>
          invokeStatic(`type`, "compare", Type.INT_TYPE, this, other)
          unless(IFLE)(block)
      }
    }

    def ifGreaterThan(other: Stack)(`then`: => Stack, `else`: => Stack): Stack = {
      assert(isPrimitive)
      assert(`type` == other.`type`)
      `type`.getSort() match {
        case Type.BOOLEAN | Type.CHAR | Type.BYTE | Type.SHORT | Type.INT =>
          ifelse(IF_ICMPGT)(`then`, `else`)
        case Type.LONG =>
          mv.visitInsn(LCMP)
          ifelse(IFGT)(`then`, `else`)
        case Type.FLOAT | Type.DOUBLE =>
          invokeStatic(`type`, "compare", Type.INT_TYPE, this, other)
          ifelse(IFGT)(`then`, `else`)
      }
    }

    def isGreaterThan(other: Stack): Stack = {
      ifGreaterThan(other)(ldc(true), ldc(false))
    }

    def unlessGreaterThan(other: Stack)(block: => Unit): Unit = {
      assert(isPrimitive)
      assert(`type` == other.`type`)
      `type`.getSort() match {
        case Type.BOOLEAN | Type.CHAR | Type.BYTE | Type.SHORT | Type.INT =>
          unless(IF_ICMPGT)(block)
        case Type.LONG =>
          mv.visitInsn(LCMP)
          unless(IFGT)(block)
        case Type.FLOAT | Type.DOUBLE =>
          invokeStatic(`type`, "compare", Type.INT_TYPE, this, other)
          unless(IFGT)(block)
      }
    }

    def ifGreaterThanOrEqual(other: Stack)(`then`: => Stack, `else`: => Stack): Stack = {
      assert(isPrimitive)
      assert(`type` == other.`type`)
      `type`.getSort() match {
        case Type.BOOLEAN | Type.CHAR | Type.BYTE | Type.SHORT | Type.INT =>
          ifelse(IF_ICMPGE)(`then`, `else`)
        case Type.LONG =>
          mv.visitInsn(LCMP)
          ifelse(IFGE)(`then`, `else`)
        case Type.FLOAT | Type.DOUBLE =>
          invokeStatic(`type`, "compare", Type.INT_TYPE, this, other)
          ifelse(IFGE)(`then`, `else`)
      }
    }

    def isGreaterThanOrEqual(other: Stack): Stack = {
      ifGreaterThanOrEqual(other)(ldc(true), ldc(false))
    }

    def unlessGreaterThanOrEqual(other: Stack)(block: => Unit): Unit = {
      assert(isPrimitive)
      assert(`type` == other.`type`)
      `type`.getSort() match {
        case Type.BOOLEAN | Type.CHAR | Type.BYTE | Type.SHORT | Type.INT =>
          unless(IF_ICMPGE)(block)
        case Type.LONG =>
          mv.visitInsn(LCMP)
          unless(IFGE)(block)
        case Type.FLOAT | Type.DOUBLE =>
          invokeStatic(`type`, "compare", Type.INT_TYPE, this, other)
          unless(IFGE)(block)
      }
    }

    def ifNull(`then`: => Stack, `else`: => Stack): Stack = {
      if (isPrimitive) {
        pop()
        `else`
      } else {
        ifelse(IFNULL)(`then`, `else`)
      }
    }

    def isNull(): Stack = {
      ifNull(ldc(true), ldc(false))
    }

    def unlessNull(block: => Unit): Unit = {
      if (isPrimitive) {
        pop()
        block
      } else {
        unless(IFNULL)(block)
      }
    }

    def ifNotNull(`then`: => Stack, `else`: => Stack): Stack = {
      ifNull(`else`, `then`)
    }

    def isNotNull(): Stack = {
      ifNotNull(ldc(true), ldc(false))
    }

    def unlessNotNull(block: => Unit): Unit = {
      if (isPrimitive) {
        pop()
      } else {
        unless(IFNONNULL)(block)
      }
    }

    def add(operand: Stack): Stack = {
      assert(isNumber)
      assert(`type` == operand.`type`)
      mv.visitInsn(`type`.getOpcode(IADD))
      this
    }

    def subtract(operand: Stack): Stack = {
      assert(isNumber)
      assert(`type` == operand.`type`)
      mv.visitInsn(`type`.getOpcode(ISUB))
      this
    }

    def multiply(operand: Stack): Stack = {
      assert(isNumber)
      assert(`type` == operand.`type`)
      mv.visitInsn(`type`.getOpcode(IMUL))
      this
    }

    def divide(operand: Stack): Stack = {
      assert(isNumber)
      assert(`type` == operand.`type`)
      mv.visitInsn(`type`.getOpcode(IDIV))
      this
    }

    def remainder(operand: Stack): Stack = {
      assert(isNumber)
      assert(`type` == operand.`type`)
      mv.visitInsn(`type`.getOpcode(IREM))
      this
    }

    def negate(): Stack = {
      assert(isNumber)
      mv.visitInsn(`type`.getOpcode(INEG))
      this
    }

    def toChar(): Stack = {
      assert(isChar || isNumber)
      `type`.getSort() match {
        case Type.CHAR => this
        case Type.BYTE | Type.SHORT | Type.INT =>
          mv.visitInsn(I2C)
          new Stack(mb, Type.CHAR_TYPE)
        case Type.LONG =>
          mv.visitInsn(L2I)
          mv.visitInsn(I2C)
          new Stack(mb, Type.CHAR_TYPE)
        case Type.FLOAT =>
          mv.visitInsn(F2I)
          mv.visitInsn(I2C)
          new Stack(mb, Type.CHAR_TYPE)
        case Type.DOUBLE =>
          mv.visitInsn(D2I)
          mv.visitInsn(I2C)
          new Stack(mb, Type.CHAR_TYPE)
      }
    }

    def toByte(): Stack = {
      assert(isChar || isNumber)
      `type`.getSort() match {
        case Type.BYTE => this
        case Type.CHAR | Type.SHORT | Type.INT =>
          mv.visitInsn(I2B)
          new Stack(mb, Type.BYTE_TYPE)
        case Type.LONG =>
          mv.visitInsn(L2I)
          mv.visitInsn(I2B)
          new Stack(mb, Type.BYTE_TYPE)
        case Type.FLOAT =>
          mv.visitInsn(F2I)
          mv.visitInsn(I2B)
          new Stack(mb, Type.BYTE_TYPE)
        case Type.DOUBLE =>
          mv.visitInsn(D2I)
          mv.visitInsn(I2B)
          new Stack(mb, Type.BYTE_TYPE)
      }
    }

    def toShort(): Stack = {
      assert(isChar || isNumber)
      `type`.getSort() match {
        case Type.SHORT => this
        case Type.CHAR | Type.BYTE | Type.INT =>
          mv.visitInsn(I2S)
          new Stack(mb, Type.SHORT_TYPE)
        case Type.LONG =>
          mv.visitInsn(L2I)
          mv.visitInsn(I2S)
          new Stack(mb, Type.SHORT_TYPE)
        case Type.FLOAT =>
          mv.visitInsn(F2I)
          mv.visitInsn(I2S)
          new Stack(mb, Type.SHORT_TYPE)
        case Type.DOUBLE =>
          mv.visitInsn(D2I)
          mv.visitInsn(I2S)
          new Stack(mb, Type.SHORT_TYPE)
      }
    }

    def toInt(): Stack = {
      assert(isBoolean || isChar || isNumber)
      `type`.getSort() match {
        case Type.BOOLEAN | Type.CHAR | Type.BYTE | Type.SHORT =>
          new Stack(mb, Type.INT_TYPE)
        case Type.INT => this
        case Type.LONG =>
          mv.visitInsn(L2I)
          new Stack(mb, Type.INT_TYPE)
        case Type.FLOAT =>
          mv.visitInsn(F2I)
          new Stack(mb, Type.INT_TYPE)
        case Type.DOUBLE =>
          mv.visitInsn(D2I)
          new Stack(mb, Type.INT_TYPE)
      }
    }

    def toLong(): Stack = {
      assert(isChar || isNumber)
      `type`.getSort() match {
        case Type.CHAR | Type.BYTE | Type.SHORT | Type.INT =>
          mv.visitInsn(I2L)
          new Stack(mb, Type.LONG_TYPE)
        case Type.LONG =>
          this
        case Type.FLOAT =>
          mv.visitInsn(F2L)
          new Stack(mb, Type.LONG_TYPE)
        case Type.DOUBLE =>
          mv.visitInsn(D2L)
          new Stack(mb, Type.LONG_TYPE)
      }
    }

    def toFloat(): Stack = {
      assert(isChar || isNumber)
      `type`.getSort() match {
        case Type.CHAR | Type.BYTE | Type.SHORT | Type.INT =>
          mv.visitInsn(I2F)
          new Stack(mb, Type.FLOAT_TYPE)
        case Type.LONG =>
          mv.visitInsn(L2F)
          new Stack(mb, Type.FLOAT_TYPE)
        case Type.FLOAT =>
          this
        case Type.DOUBLE =>
          mv.visitInsn(D2F)
          new Stack(mb, Type.FLOAT_TYPE)
      }
    }

    def toDouble(): Stack = {
      assert(isChar || isNumber)
      `type`.getSort() match {
        case Type.CHAR | Type.BYTE | Type.SHORT | Type.INT =>
          mv.visitInsn(I2D)
          new Stack(mb, Type.DOUBLE_TYPE)
        case Type.LONG =>
          mv.visitInsn(L2D)
          new Stack(mb, Type.DOUBLE_TYPE)
        case Type.FLOAT =>
          mv.visitInsn(F2D)
          new Stack(mb, Type.DOUBLE_TYPE)
        case Type.DOUBLE =>
          this
      }
    }
  }

  class Var private[MethodBuilder] (mb: MethodBuilder, `type`: Type, val local: Int) extends Value(`type`) {
    import mb._

    def push(): Stack = {
      mv.visitVarInsn(`type`.getOpcode(ILOAD), local)
      new Stack(mb, `type`)
    }

    def inc(increment: Int): Var = {
      assert(isInteger)
      mv.visitIincInsn(local, increment)
      this
    }

    lazy val nextLocal: Int = {
      local + size
    }
  }
}
