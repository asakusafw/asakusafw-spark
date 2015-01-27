package com.asakusafw.spark.tools

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

import org.objectweb.asm.Type

package object asm {

  implicit class AugmentedType(val `type`: Type) extends AnyVal {

    def boxed: Type = {
      `type`.getSort() match {
        case Type.BOOLEAN => classOf[JBoolean].asType
        case Type.CHAR    => classOf[JChar].asType
        case Type.BYTE    => classOf[JByte].asType
        case Type.SHORT   => classOf[JShort].asType
        case Type.INT     => classOf[JInt].asType
        case Type.LONG    => classOf[JLong].asType
        case Type.FLOAT   => classOf[JFloat].asType
        case Type.DOUBLE  => classOf[JDouble].asType
        case _            => `type`
      }
    }

    def isPrimitive: Boolean = {
      isBoolean || isChar || isNumber
    }

    def isBoolean: Boolean = {
      `type`.getSort() == Type.BOOLEAN
    }

    def isChar: Boolean = {
      `type`.getSort() == Type.CHAR
    }

    def isInteger: Boolean = {
      (`type`.getSort() == Type.BYTE
        || `type`.getSort() == Type.SHORT
        || `type`.getSort() == Type.INT)
    }

    def isLong: Boolean = {
      `type`.getSort() == Type.LONG
    }

    def isFloat: Boolean = {
      `type`.getSort() == Type.FLOAT
    }

    def isDouble: Boolean = {
      `type`.getSort() == Type.DOUBLE
    }

    def isNumber: Boolean = {
      isInteger || isLong || isFloat || isDouble
    }

    def isArray: Boolean = {
      `type`.getSort() == Type.ARRAY
    }
  }

  implicit class AsmClass[A](val cls: Class[A]) extends AnyVal {

    def boxed: Class[_] = AsmClass.boxed.getOrElse(cls, cls)

    def unboxed: Class[_] = AsmClass.unboxed.getOrElse(cls, cls)

    def asType: Type = Type.getType(cls)

    def asBoxedType: Type = boxed.asType

    def asUnboxedType: Type = unboxed.asType

    def getInternalName(): String = asType.getInternalName()
  }

  private object AsmClass {

    val boxed = Map[Class[_], Class[_]](
      classOf[Boolean] -> classOf[JBoolean],
      classOf[Char] -> classOf[JChar],
      classOf[Byte] -> classOf[JByte],
      classOf[Short] -> classOf[JShort],
      classOf[Int] -> classOf[JInt],
      classOf[Long] -> classOf[JLong],
      classOf[Float] -> classOf[JFloat],
      classOf[Double] -> classOf[JDouble])

    val unboxed = Map[Class[_], Class[_]](
      classOf[JBoolean] -> classOf[Boolean],
      classOf[JChar] -> classOf[Char],
      classOf[JByte] -> classOf[Byte],
      classOf[JShort] -> classOf[Short],
      classOf[JInt] -> classOf[Int],
      classOf[JLong] -> classOf[Long],
      classOf[JFloat] -> classOf[Float],
      classOf[JDouble] -> classOf[Double])
  }

  implicit val emptyAnnotationBuilderBlock = ClassBuilder.emptyAnnotationBuilderBlock
  implicit val emptyFieldBuilderBlock = ClassBuilder.emptyFieldBuilderBlock

  implicit val emptyTypeArgumentSignatureBuilderBlock = TypeSignatureBuilder.emptyTypeArgumentSignatureBuilderBlock
}
