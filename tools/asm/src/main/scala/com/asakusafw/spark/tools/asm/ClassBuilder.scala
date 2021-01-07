/*
 * Copyright 2011-2021 Asakusa Framework Team.
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

import java.io.PrintWriter
import java.io.StringWriter
import java.util.Arrays
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable

import org.objectweb.asm.ClassReader
import org.objectweb.asm.ClassVisitor
import org.objectweb.asm.ClassWriter
import org.objectweb.asm.Opcodes._
import org.objectweb.asm.Type
import org.objectweb.asm.util.TraceClassVisitor
import org.slf4j.LoggerFactory

import com.asakusafw.spark.tools.asm.MethodBuilder._

abstract class ClassBuilder(
  val thisType: Type,
  _signature: Option[ClassSignatureBuilder],
  val superType: Type,
  _interfaceTypes: Type*) {

  def signature: Option[ClassSignatureBuilder] = _signature

  def interfaceTypes: Seq[Type] = _interfaceTypes

  val Logger = LoggerFactory.getLogger(getClass)

  def this(
    thisType: Type, signature: ClassSignatureBuilder, superType: Type, interfaceTypes: Type*) =
    this(thisType, Option(signature), superType, interfaceTypes: _*)

  def this(thisType: Type, superType: Type, interfaceTypes: Type*) =
    this(thisType, None, superType, interfaceTypes: _*)

  final def build(): Array[Byte] = {
    val cw = new ClassWriter(ClassWriter.COMPUTE_MAXS | ClassWriter.COMPUTE_FRAMES)
    cw.visit(
      V1_7,
      ACC_PUBLIC,
      thisType.getInternalName(),
      signature.map(_.build()).orNull,
      superType.getInternalName(),
      interfaceTypes.map(_.getInternalName()).toArray)
    defAnnotations(new AnnotationDef(cw))
    defFields(new FieldDef(cw))
    defConstructors(new ConstructorDef(cw))
    val methodDef = new MethodDef(cw)
    defMethods(methodDef)
    if (definingToString) {
      defToString0(methodDef)
    }
    if (definingHashCode) {
      defHashCode0(methodDef)
    }
    if (definingEquals) {
      defEquals0(methodDef)
    }
    cw.visitEnd()
    val bytes = cw.toByteArray()
    if (Logger.isTraceEnabled) {
      Logger.trace {
        val writer = new StringWriter
        val cr = new ClassReader(bytes)
        cr.accept(new TraceClassVisitor(new PrintWriter(writer)), 0)
        writer.toString
      }
    }
    bytes
  }

  def defAnnotations(annotationDef: AnnotationDef): Unit = {
  }
  def defFields(fieldDef: FieldDef): Unit = {
  }
  def defConstructors(ctorDef: ConstructorDef): Unit = {
    ctorDef.newInit(Seq.empty) { implicit mb =>
      val thisVar :: _ = mb.argVars
      thisVar.push().invokeInit(superType)
    }
  }
  def defMethods(methodDef: MethodDef): Unit = {
  }

  final class AnnotationDef private[ClassBuilder] (cv: ClassVisitor) {

    final def newAnnotation(
      desc: String,
      visible: Boolean)(implicit block: AnnotationBuilder => Unit): Unit = {
      val av = cv.visitAnnotation(desc, visible)
      try {
        block(new AnnotationBuilder(av))
      } finally {
        av.visitEnd()
      }
    }
  }

  private val fields = mutable.ArrayBuffer.empty[(String, Type)]

  final class FieldDef private[ClassBuilder] (cv: ClassVisitor) {

    final def addField(name: String, `type`: Type): Unit = {
      fields += ((name, `type`))
    }

    final def newField(
      name: String,
      `type`: Type)(implicit block: FieldBuilder => Unit): Unit = {
      newField(ACC_PUBLIC, name, `type`, None)(block)
    }

    final def newStaticField(
      name: String,
      `type`: Type)(implicit block: FieldBuilder => Unit): Unit = {
      newField(ACC_PUBLIC | ACC_STATIC, name, `type`, None)(block)
    }

    final def newFinalField(
      name: String,
      `type`: Type)(implicit block: FieldBuilder => Unit): Unit = {
      newField(ACC_PUBLIC | ACC_FINAL, name, `type`, None)(block)
    }

    final def newStaticFinalField(
      name: String,
      `type`: Type)(implicit block: FieldBuilder => Unit): Unit = {
      newField(ACC_PUBLIC | ACC_STATIC | ACC_FINAL, name, `type`, None)(block)
    }

    final def newField(
      name: String,
      `type`: Type,
      signature: TypeSignatureBuilder)(implicit block: FieldBuilder => Unit): Unit = {
      newField(ACC_PUBLIC, name, `type`, Option(signature))(block)
    }

    final def newStaticField(
      name: String,
      `type`: Type,
      signature: TypeSignatureBuilder)(implicit block: FieldBuilder => Unit): Unit = {
      newField(ACC_PUBLIC | ACC_STATIC, name, `type`, Option(signature))(block)
    }

    final def newFinalField(
      name: String,
      `type`: Type,
      signature: TypeSignatureBuilder)(implicit block: FieldBuilder => Unit): Unit = {
      newField(ACC_PUBLIC | ACC_FINAL, name, `type`, Option(signature))(block)
    }

    final def newStaticFinalField(
      name: String,
      `type`: Type,
      signature: TypeSignatureBuilder)(implicit block: FieldBuilder => Unit): Unit = {
      newField(ACC_PUBLIC | ACC_STATIC | ACC_FINAL, name, `type`, Option(signature))(block)
    }

    final def newField(
      access: Int,
      name: String,
      `type`: Type)(implicit block: FieldBuilder => Unit): Unit = {
      newField(access, name, `type`, None)(block)
    }

    final def newField(
      access: Int,
      name: String,
      `type`: Type,
      signature: TypeSignatureBuilder)(implicit block: FieldBuilder => Unit): Unit = {
      newField(access, name, `type`, Option(signature))(block)
    }

    final def newField(
      access: Int,
      name: String,
      `type`: Type,
      signature: Option[TypeSignatureBuilder])(implicit block: FieldBuilder => Unit): Unit = {
      if ((access & ACC_STATIC) == 0) {
        addField(name, `type`)
      }
      val fv = cv.visitField(
        access, name, `type`.getDescriptor(), signature.map(_.build()).orNull, null) // scalastyle:ignore
      try {
        block(new FieldBuilder(fv))
      } finally {
        fv.visitEnd()
      }
    }
  }

  final class ConstructorDef private[ClassBuilder] (cv: ClassVisitor) {
    private val methodDef = new MethodDef(cv)

    final def newInit(
      argumentTypes: Seq[Type],
      exceptionTypes: Type*)(block: MethodBuilder => Unit): Unit = {
      newInit(ACC_PUBLIC, argumentTypes, None, exceptionTypes: _*)(block)
    }

    final def newInit(
      argumentTypes: Seq[Type],
      signature: MethodSignatureBuilder,
      exceptionTypes: Type*)(block: MethodBuilder => Unit): Unit = {
      newInit(ACC_PUBLIC, argumentTypes, Option(signature), exceptionTypes: _*)(block)
    }

    final def newInit(
      argumentTypes: Seq[Type],
      signature: Option[MethodSignatureBuilder],
      exceptionTypes: Type*)(block: MethodBuilder => Unit): Unit = {
      newInit(ACC_PUBLIC, argumentTypes, signature, exceptionTypes: _*)(block)
    }

    final def newInit(
      access: Int,
      argumentTypes: Seq[Type],
      exceptionTypes: Type*)(block: MethodBuilder => Unit): Unit = {
      newInit(access, argumentTypes, None, exceptionTypes: _*)(block)
    }

    final def newInit(
      access: Int,
      argumentTypes: Seq[Type],
      signature: MethodSignatureBuilder,
      exceptionTypes: Type*)(block: MethodBuilder => Unit): Unit = {
      newInit(access, argumentTypes, Option(signature), exceptionTypes: _*)(block)
    }

    final def newInit(
      access: Int,
      argumentTypes: Seq[Type],
      signature: Option[MethodSignatureBuilder],
      exceptionTypes: Type*)(block: MethodBuilder => Unit): Unit = {
      methodDef.newMethod(
        access, "<init>", argumentTypes, signature, exceptionTypes: _*) { implicit mb =>
          block(mb)
          `return`()
        }
    }

    var staticInitialized = false

    final def newStaticInit(block: MethodBuilder => Unit): Unit = {
      assert(!staticInitialized, "Static initializer should be defined only once.")
      methodDef.newMethod(
        ACC_STATIC, "<clinit>", Type.VOID_TYPE, Array.empty[Type]) { implicit mb =>
          block(mb)
          `return`()
        }
      staticInitialized = true
    }
  }

  final class MethodDef private[ClassBuilder] (cv: ClassVisitor) {

    final def newMethod(
      name: String,
      argumentTypes: Seq[Type],
      exceptionTypes: Type*)(block: MethodBuilder => Unit): Unit = {
      newMethod(
        ACC_PUBLIC, name, Type.VOID_TYPE, argumentTypes, None, exceptionTypes: _*)(block)
    }

    final def newMethod(
      name: String,
      retType: Type,
      argumentTypes: Seq[Type],
      exceptionTypes: Type*)(block: MethodBuilder => Unit): Unit = {
      newMethod(
        ACC_PUBLIC, name, retType, argumentTypes, None, exceptionTypes: _*)(block)
    }

    final def newStaticMethod(
      name: String,
      argumentTypes: Seq[Type],
      exceptionTypes: Type*)(block: MethodBuilder => Unit): Unit = {
      newMethod(
        ACC_PUBLIC | ACC_STATIC,
        name, Type.VOID_TYPE, argumentTypes, None, exceptionTypes: _*)(block)
    }

    final def newStaticMethod(
      name: String,
      retType: Type,
      argumentTypes: Seq[Type],
      exceptionTypes: Type*)(block: MethodBuilder => Unit): Unit = {
      newMethod(
        ACC_PUBLIC | ACC_STATIC,
        name, retType, argumentTypes, None, exceptionTypes: _*)(block)
    }

    final def newMethod(
      name: String,
      argumentTypes: Seq[Type],
      signature: MethodSignatureBuilder,
      exceptionTypes: Type*)(block: MethodBuilder => Unit): Unit = {
      newMethod(
        ACC_PUBLIC,
        name, Type.VOID_TYPE, argumentTypes, Option(signature), exceptionTypes: _*)(block)
    }

    final def newMethod(
      name: String,
      argumentTypes: Seq[Type],
      signature: Option[MethodSignatureBuilder],
      exceptionTypes: Type*)(block: MethodBuilder => Unit): Unit = {
      newMethod(
        ACC_PUBLIC, name, Type.VOID_TYPE, argumentTypes, signature, exceptionTypes: _*)(block)
    }

    final def newMethod(
      name: String,
      retType: Type,
      argumentTypes: Seq[Type],
      signature: MethodSignatureBuilder,
      exceptionTypes: Type*)(block: MethodBuilder => Unit): Unit = {
      newMethod(
        ACC_PUBLIC, name, retType, argumentTypes, Option(signature), exceptionTypes: _*)(block)
    }

    final def newMethod(
      name: String,
      retType: Type,
      argumentTypes: Seq[Type],
      signature: Option[MethodSignatureBuilder],
      exceptionTypes: Type*)(block: MethodBuilder => Unit): Unit = {
      newMethod(
        ACC_PUBLIC, name, retType, argumentTypes, signature, exceptionTypes: _*)(block)
    }

    final def newStaticMethod(
      name: String,
      argumentTypes: Seq[Type],
      signature: MethodSignatureBuilder,
      exceptionTypes: Type*)(block: MethodBuilder => Unit): Unit = {
      newMethod(
        ACC_PUBLIC | ACC_STATIC,
        name, Type.VOID_TYPE, argumentTypes, Option(signature), exceptionTypes: _*)(block)
    }

    final def newStaticMethod(
      name: String,
      argumentTypes: Seq[Type],
      signature: Option[MethodSignatureBuilder],
      exceptionTypes: Type*)(block: MethodBuilder => Unit): Unit = {
      newMethod(
        ACC_PUBLIC | ACC_STATIC,
        name, Type.VOID_TYPE, argumentTypes, signature, exceptionTypes: _*)(block)
    }

    final def newStaticMethod(
      name: String,
      retType: Type,
      argumentTypes: Seq[Type],
      signature: MethodSignatureBuilder,
      exceptionTypes: Type*)(block: MethodBuilder => Unit): Unit = {
      newMethod(
        ACC_PUBLIC | ACC_STATIC,
        name, retType, argumentTypes, Option(signature), exceptionTypes: _*)(block)
    }

    final def newStaticMethod(
      name: String,
      retType: Type,
      argumentTypes: Seq[Type],
      signature: Option[MethodSignatureBuilder],
      exceptionTypes: Type*)(block: MethodBuilder => Unit): Unit = {
      newMethod(
        ACC_PUBLIC | ACC_STATIC,
        name, retType, argumentTypes, signature, exceptionTypes: _*)(block)
    }

    final def newMethod(
      access: Int,
      name: String,
      argumentTypes: Seq[Type],
      exceptionTypes: Type*)(block: MethodBuilder => Unit): Unit = {
      newMethod(
        access, name, Type.VOID_TYPE, argumentTypes, None, exceptionTypes: _*)(block)
    }

    final def newMethod(
      access: Int,
      name: String,
      retType: Type,
      argumentTypes: Seq[Type],
      exceptionTypes: Type*)(block: MethodBuilder => Unit): Unit = {
      newMethod(
        access, name, retType, argumentTypes, None, exceptionTypes: _*)(block)
    }

    final def newMethod(
      access: Int,
      name: String,
      argumentTypes: Seq[Type],
      signature: MethodSignatureBuilder,
      exceptionTypes: Type*)(block: MethodBuilder => Unit): Unit = {
      newMethod(
        access, name, Type.VOID_TYPE, argumentTypes, Option(signature), exceptionTypes: _*)(block)
    }

    final def newMethod(
      access: Int,
      name: String,
      argumentTypes: Seq[Type],
      signature: Option[MethodSignatureBuilder],
      exceptionTypes: Type*)(block: MethodBuilder => Unit): Unit = {
      newMethod(
        access, name, Type.VOID_TYPE, argumentTypes, signature, exceptionTypes: _*)(block)
    }

    final def newMethod(
      access: Int,
      name: String,
      retType: Type,
      argumentTypes: Seq[Type],
      signature: MethodSignatureBuilder,
      exceptionTypes: Type*)(block: MethodBuilder => Unit): Unit = {
      newMethod(
        access, name, retType, argumentTypes, Option(signature), exceptionTypes: _*)(block)
    }

    final def newMethod(
      access: Int,
      name: String,
      retType: Type,
      argumentTypes: Seq[Type],
      signature: Option[MethodSignatureBuilder],
      exceptionTypes: Type*)(block: MethodBuilder => Unit): Unit = {
      newMethod(
        access,
        name,
        Type.getMethodType(retType, argumentTypes: _*),
        signature,
        exceptionTypes: _*)(block)
    }

    final def newMethod(
      name: String,
      methodType: Type,
      exceptionTypes: Type*)(block: MethodBuilder => Unit): Unit = {
      newMethod(
        ACC_PUBLIC, name, methodType, None, exceptionTypes: _*)(block)
    }

    final def newMethod(
      name: String,
      methodType: Type,
      signature: MethodSignatureBuilder,
      exceptionTypes: Type*)(block: MethodBuilder => Unit): Unit = {
      newMethod(
        ACC_PUBLIC, name, methodType, Option(signature), exceptionTypes: _*)(block)
    }

    final def newMethod(
      name: String,
      methodType: Type,
      signature: Option[MethodSignatureBuilder],
      exceptionTypes: Type*)(block: MethodBuilder => Unit): Unit = {
      newMethod(
        ACC_PUBLIC, name, methodType, signature, exceptionTypes: _*)(block)
    }

    final def newStaticMethod(
      name: String,
      methodType: Type,
      exceptionTypes: Type*)(block: MethodBuilder => Unit): Unit = {
      newMethod(
        ACC_PUBLIC | ACC_STATIC,
        name, methodType, None, exceptionTypes: _*)(block)
    }

    final def newStaticMethod(
      name: String,
      methodType: Type,
      signature: MethodSignatureBuilder,
      exceptionTypes: Type*)(block: MethodBuilder => Unit): Unit = {
      newMethod(
        ACC_PUBLIC | ACC_STATIC,
        name, methodType, Option(signature), exceptionTypes: _*)(block)
    }

    final def newStaticMethod(
      name: String,
      methodType: Type,
      signature: Option[MethodSignatureBuilder],
      exceptionTypes: Type*)(block: MethodBuilder => Unit): Unit = {
      newMethod(
        ACC_PUBLIC | ACC_STATIC,
        name, methodType, signature, exceptionTypes: _*)(block)
    }

    final def newMethod(
      access: Int,
      name: String,
      methodType: Type,
      exceptionTypes: Type*)(block: MethodBuilder => Unit): Unit = {
      newMethod(
        access, name, methodType, None, exceptionTypes: _*)(block)
    }

    final def newMethod(
      access: Int,
      name: String,
      methodType: Type,
      signature: MethodSignatureBuilder,
      exceptionTypes: Type*)(block: MethodBuilder => Unit): Unit = {
      newMethod(
        access, name, methodType, Option(signature), exceptionTypes: _*)(block)
    }

    final def newMethod(
      access: Int,
      name: String,
      methodType: Type,
      signature: Option[MethodSignatureBuilder],
      exceptionTypes: Type*)(block: MethodBuilder => Unit): Unit = {
      assert(methodType.getSort == Type.METHOD)
      val mv = cv.visitMethod(
        access,
        name,
        methodType.getDescriptor,
        signature.map(_.build()).orNull,
        exceptionTypes.map(_.getInternalName()).toArray)
      mv.visitCode()
      try {
        val nextLocal = new AtomicInteger(0)
        val argVars = (if ((access & ACC_STATIC) == 0) {
          List(Var(thisType, nextLocal.getAndAdd(thisType.getSize)))
        } else {
          List.empty[Var]
        }) ++ methodType.getArgumentTypes.map { t =>
          Var(t, nextLocal.getAndAdd(t.getSize))
        }
        block(MethodBuilder(thisType, access, name, methodType, argVars)(mv, nextLocal))
        mv.visitMaxs(0, 0)
      } finally {
        mv.visitEnd()
      }
    }
  }

  private var definingToString: Boolean = false
  private var definingHashCode: Boolean = false
  private var definingEquals: Boolean = false

  protected def defToString(): Unit = {
    definingToString = true
  }
  protected def defHashCode(): Unit = {
    definingHashCode = true
  }
  protected def defEquals(): Unit = {
    definingEquals = true
  }

  private final def defToString0(methodDef: MethodDef): Unit = {
    methodDef.newMethod("toString", classOf[String].asType, Seq.empty[Type]) { implicit mb =>
      val thisVar :: _ = mb.argVars

      val builder = pushNew0(classOf[StringBuilder].asType)

      val className = ldc(
        thisType.getClassName().substring(thisType.getClassName().lastIndexOf(".") + 1))
      builder.invokeV("append", classOf[StringBuilder].asType, className)

      builder.invokeV("append", classOf[StringBuilder].asType, ldc("("))

      fields.zipWithIndex.foreach {
        case ((name, fieldType), idx) =>
          if (idx > 0) {
            builder.invokeV("append", classOf[StringBuilder].asType, ldc(","))
          }
          val fieldValue = thisVar.push().getField(name, fieldType)
          if (fieldValue.isChar) {
            builder.invokeV("append", classOf[StringBuilder].asType, fieldValue)
          } else if (fieldValue.isInteger) {
            builder.invokeV("append", classOf[StringBuilder].asType,
              fieldValue.asType(Type.INT_TYPE))
          } else if (fieldValue.isPrimitive) {
            builder.invokeV("append", classOf[StringBuilder].asType, fieldValue)
          } else if (fieldValue.isArray) {
            builder.invokeV("append", classOf[StringBuilder].asType,
              if (fieldType.getDimensions() > 1) {
                invokeStatic(
                  classOf[Arrays].asType,
                  "deepToString",
                  classOf[String].asType,
                  fieldValue.asType(classOf[Array[AnyRef]].asType))
              } else if (fieldType.getElementType().getSort() < Type.ARRAY) {
                invokeStatic(
                  classOf[Arrays].asType,
                  "toString",
                  classOf[String].asType,
                  fieldValue)
              } else {
                invokeStatic(
                  classOf[Arrays].asType,
                  "toString",
                  classOf[String].asType,
                  fieldValue.asType(classOf[Array[AnyRef]].asType))
              })
          } else {
            builder.invokeV(
              "append",
              classOf[StringBuilder].asType,
              fieldValue.asType(classOf[AnyRef].asType))
          }
      }

      builder.invokeV("append", classOf[StringBuilder].asType, ldc(")"))

      `return`(builder.invokeV("toString", classOf[String].asType))
    }
  }

  private final def defHashCode0(methodDef: MethodDef): Unit = {
    methodDef.newMethod("hashCode", Type.INT_TYPE, Seq.empty[Type]) { implicit mb =>
      val thisVar :: _ = mb.argVars
      val Prime = 31
      val result = ldc(1)
      fields.foreach {
        case (name, fieldType) =>
          result.multiply(ldc(Prime))
          val fieldValue = thisVar.push().getField(name, fieldType)
          val fieldHashCode = if (fieldValue.isPrimitive) {
            fieldValue.box().invokeV("hashCode", Type.INT_TYPE)
          } else if (fieldValue.isArray) {
            if (fieldType.getDimensions() > 1) {
              invokeStatic(
                classOf[Arrays].asType,
                "deepHashCode",
                Type.INT_TYPE,
                fieldValue.asType(classOf[Array[AnyRef]].asType))
            } else if (fieldType.getElementType().getSort() < Type.ARRAY) {
              invokeStatic(
                classOf[Arrays].asType,
                "hashCode",
                Type.INT_TYPE,
                fieldValue)
            } else {
              invokeStatic(
                classOf[Arrays].asType,
                "hashCode",
                Type.INT_TYPE,
                fieldValue.asType(classOf[Array[AnyRef]].asType))
            }
          } else {
            fieldValue.dup().ifNull({
              fieldValue.pop()
              ldc(0)
            }, {
              fieldValue.invokeV("hashCode", Type.INT_TYPE)
            })
          }
          result.add(fieldHashCode)
      }
      `return`(result)
    }
  }

  private final def defEquals0(methodDef: MethodDef): Unit = {
    methodDef.newMethod("equals", Type.BOOLEAN_TYPE, Seq(classOf[AnyRef].asType)) { implicit mb =>
      val thisVar :: otherVar :: _ = mb.argVars

      val result = thisVar.push().ifEq(otherVar.push())(ldc(true), {
        otherVar.push().ifNull(ldc(false), {
          val thisClass = thisVar.push().invokeV("getClass", classOf[Class[_]].asType)
          val otherClass = otherVar.push().invokeV("getClass", classOf[Class[_]].asType)
          thisClass.ifEq(otherClass)({
            if (fields.isEmpty) {
              ldc(true)
            } else {
              val objVar = otherVar.push().cast(thisVar.`type`).store()

              def equals0(head: (String, Type), tail: Seq[(String, Type)]): MethodBuilder.Stack = {
                val fieldValue = thisVar.push().getField(head._1, head._2)
                if (fieldValue.isPrimitive) {
                  val otherFieldValue = objVar.push().getField(head._1, head._2)
                  fieldValue.ifEq(otherFieldValue)({
                    if (tail.isEmpty) {
                      ldc(true)
                    } else {
                      equals0(tail.head, tail.tail)
                    }
                  }, ldc(false))
                } else {
                  val eq = fieldValue.dup().ifNull({
                    fieldValue.pop()
                    val otherFieldValue = objVar.push().getField(head._1, head._2)
                    otherFieldValue.isNull()
                  }, {
                    val otherFieldValue = objVar.push().getField(head._1, head._2)
                    fieldValue.isEqual(otherFieldValue)
                  })
                  if (tail.isEmpty) {
                    eq
                  } else {
                    eq.ifTrue({
                      equals0(tail.head, tail.tail)
                    }, ldc(false))
                  }
                }
              }

              equals0(fields.head, fields.tail)
            }
          }, ldc(false))
        })
      })

      `return`(result)
    }
  }
}

object ClassBuilder {
  implicit val emptyAnnotationBuilderBlock: AnnotationBuilder => Unit = { ab => }
  implicit val emptyFieldBuilderBlock: FieldBuilder => Unit = { fb => }
}
