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

import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner

import java.util.Arrays

import org.objectweb.asm.Opcodes._
import org.objectweb.asm.Type

import com.asakusafw.spark.tools.asm.MethodBuilder._

@RunWith(classOf[JUnitRunner])
class ClassBuilderSpecTest extends ClassBuilderSpec

class ClassBuilderSpec extends FlatSpec with LoadClassSugar {

  "ClassBuilder" should "build a class" in {
    val builder = new TestClassBuilder

    val cls = loadClass(builder.thisType.getClassName, builder.build())
    assert(cls.getField("BOOLEAN").get(null) === true)

    val instance = cls.newInstance
    assert(instance.toString === "TestClass(false,0,0,0,0,0,0.0,0.0,null,null,null,null,null,null,null,null,null,null,null,null)")
    assert(instance.hashCode === ((1 * 31 + false.hashCode) * 31 + '0'.hashCode) * 31 * 31 * 31 * 31 * 31 * 31 * 31 * 31 * 31 * 31 * 31 * 31 * 31 * 31 * 31 * 31 * 31 * 31)
    assert(instance === cls.newInstance)
    assert(cls.getMethod("testLoop").invoke(instance) === 10)
    assert(cls.getMethod("testWhileLoop").invoke(instance) === 10)
    assert(cls.getMethod("testDoWhile").invoke(instance) === 12)
    assert(cls.getMethod("testTryCatch").invoke(instance) === 1)

    val other1 = cls.newInstance
    cls.getField("Integer").set(other1, 1)
    assert(other1.toString === "TestClass(false,0,0,0,0,0,0.0,0.0,null,null,null,null,1,null,null,null,null,null,null,null)")
    assert(other1.hashCode === (((1 * 31 + false.hashCode) * 31 + '0'.hashCode) * 31 * 31 * 31 * 31 * 31 * 31 * 31 * 31 * 31 * 31 * 31 + 1) * 31 * 31 * 31 * 31 * 31 * 31 * 31)
    assert(instance !== other1)

    val other2 = cls.newInstance
    cls.getField("Binary").set(other2, Array(1.toByte))
    assert(other2.toString === "TestClass(false,0,0,0,0,0,0.0,0.0,null,null,null,null,null,null,null,null,null,[1],null,null)")
    assert(other2.hashCode === (((1 * 31 + false.hashCode) * 31 + '0'.hashCode) * 31 * 31 * 31 * 31 * 31 * 31 * 31 * 31 * 31 * 31 * 31 * 31 * 31 * 31 * 31 * 31 + Arrays.hashCode(Array(1.toByte))) * 31 * 31)
    assert(instance !== other2)

    val other3 = cls.newInstance
    cls.getField("StringArray").set(other3, Array("abc"))
    assert(other3.toString === "TestClass(false,0,0,0,0,0,0.0,0.0,null,null,null,null,null,null,null,null,null,null,[abc],null)")
    assert(other3.hashCode === (((1 * 31 + false.hashCode) * 31 + '0'.hashCode) * 31 * 31 * 31 * 31 * 31 * 31 * 31 * 31 * 31 * 31 * 31 * 31 * 31 * 31 * 31 * 31 * 31 + Arrays.hashCode(Array[AnyRef]("abc"))) * 31)
    assert(instance !== other3)

    val other4 = cls.newInstance
    cls.getField("BinaryArray").set(other4, Array(Array(1.toByte)))
    assert(other4.toString === "TestClass(false,0,0,0,0,0,0.0,0.0,null,null,null,null,null,null,null,null,null,null,null,[[1]])")
    assert(other4.hashCode === (((1 * 31 + false.hashCode) * 31 + '0'.hashCode) * 31 * 31 * 31 * 31 * 31 * 31 * 31 * 31 * 31 * 31 * 31 * 31 * 31 * 31 * 31 * 31 * 31 * 31 + Arrays.deepHashCode(Array(Array(1.toByte)))))
    assert(instance !== other4)
  }
}

class TestClassBuilder
  extends ClassBuilder(Type.getType("Lsparktest/query/asm/TestClass;"), classOf[AnyRef].asType) {

  defToString()
  defHashCode()
  defEquals()

  override def defFields(fieldDef: FieldDef): Unit = {
    fieldDef.newStaticFinalField("BOOLEAN", Type.BOOLEAN_TYPE)
    fieldDef.newStaticFinalField("CHAR", Type.CHAR_TYPE)
    fieldDef.newStaticFinalField("BYTE", Type.BYTE_TYPE)
    fieldDef.newStaticFinalField("SHORT", Type.SHORT_TYPE)
    fieldDef.newStaticFinalField("INT", Type.INT_TYPE)
    fieldDef.newStaticFinalField("LONG", Type.LONG_TYPE)
    fieldDef.newStaticFinalField("FLOAT", Type.FLOAT_TYPE)
    fieldDef.newStaticFinalField("DOUBLE", Type.DOUBLE_TYPE)

    fieldDef.newField("boolean", Type.BOOLEAN_TYPE)
    fieldDef.newField("char", Type.CHAR_TYPE)
    fieldDef.newField("byte", Type.BYTE_TYPE)
    fieldDef.newField("short", Type.SHORT_TYPE)
    fieldDef.newField("int", Type.INT_TYPE)
    fieldDef.newField("long", Type.LONG_TYPE)
    fieldDef.newField("float", Type.FLOAT_TYPE)
    fieldDef.newField("double", Type.DOUBLE_TYPE)

    fieldDef.newField("Boolean", classOf[java.lang.Boolean].asType)
    fieldDef.newField("Char", classOf[java.lang.Character].asType)
    fieldDef.newField("Byte", classOf[java.lang.Byte].asType)
    fieldDef.newField("Short", classOf[java.lang.Short].asType)
    fieldDef.newField("Integer", classOf[java.lang.Integer].asType)
    fieldDef.newField("Long", classOf[java.lang.Long].asType)
    fieldDef.newField("Float", classOf[java.lang.Float].asType)
    fieldDef.newField("Double", classOf[java.lang.Double].asType)
    fieldDef.newField("String", classOf[java.lang.String].asType)

    fieldDef.newField("Binary", classOf[Array[Byte]].asType)
    fieldDef.newField("StringArray", classOf[Array[String]].asType)
    fieldDef.newField("BinaryArray", classOf[Array[Array[Byte]]].asType)
  }

  override def defConstructors(ctorDef: ConstructorDef): Unit = {
    ctorDef.newInit(Seq.empty) { implicit mb =>
      val thisVar :: _ = mb.argVars
      thisVar.push().invokeInit(superType)
      thisVar.push().putField("char", ldc('0'))
    }

    ctorDef.newStaticInit { implicit mb =>
      putStatic(thisType, "BOOLEAN", Type.BOOLEAN_TYPE, ldc(true))
    }
  }

  override def defMethods(methodDef: MethodDef): Unit = {
    super.defMethods(methodDef)

    methodDef.newMethod("testLoop", Type.INT_TYPE, Seq.empty) { implicit mb =>
      val thisVar :: _ = mb.argVars
      val iVar = ldc(0).store()
      loop { ctrl =>
        iVar.push().unlessLessThan(ldc(10))(ctrl.break())
        iVar.push().add(ldc(1)).store(iVar.local)
      }
      `return`(iVar.push())
    }

    methodDef.newMethod("testWhileLoop", Type.INT_TYPE, Seq.empty) { implicit mb =>
      val thisVar :: _ = mb.argVars
      val iVar = ldc(0).store()
      whileLoop(iVar.push().isLessThan(ldc(10))) { _ =>
        iVar.push().add(ldc(1)).store(iVar.local)
      }
      `return`(iVar.push())
    }

    methodDef.newMethod("testDoWhile", Type.INT_TYPE, Seq.empty) { implicit mb =>
      val thisVar :: _ = mb.argVars
      val iVar = ldc(10).store()
      doWhile { _ =>
        iVar.push().add(ldc(2)).store(iVar.local)
      }(iVar.push().isLessThan(ldc(10)))
      `return`(iVar.push())
    }

    methodDef.newMethod("testTryCatch", Type.INT_TYPE, Seq.empty) { implicit mb =>
      val thisVar :: _ = mb.argVars
      val iVar = ldc(0).store()
      tryCatch({
        invokeStatic(classOf[Integer].asType, "parseInt", Type.INT_TYPE, ldc("abc")).store(iVar.local)
      }, (classOf[NumberFormatException].asType, { e =>
        e.pop()
        ldc(1).store(iVar.local)
      }))
      `return`(iVar.push())
    }
  }
}
