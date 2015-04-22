package com.asakusafw.spark.compiler
package serializer

import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable

import org.apache.hadoop.io.Writable
import com.esotericsoftware.kryo._
import com.esotericsoftware.kryo.io._
import org.objectweb.asm.Type
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.lang.compiler.api.JobflowProcessor.{ Context => JPContext }
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.spark.runtime.rdd.BranchKey
import com.asakusafw.spark.tools.asm._

class BranchKeySerializerClassBuilder(
  flowId: String,
  branchKeysType: Type)
    extends ClassBuilder(
      Type.getType(s"L${GeneratedClassPackageInternalName}/${flowId}/serializer/BranchKeySerializer;"),
      new ClassSignatureBuilder()
        .newSuperclass {
          _.newClassType(classOf[Serializer[_]].asType) {
            _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[BranchKey].asType)
          }
        }
        .build(),
      classOf[Serializer[_]].asType) {

  override def defMethods(methodDef: MethodDef): Unit = {
    super.defMethods(methodDef)

    methodDef.newMethod("write", Seq(classOf[Kryo].asType, classOf[Output].asType, classOf[AnyRef].asType)) { mb =>
      import mb._
      val kryoVar = `var`(classOf[Kryo].asType, thisVar.nextLocal)
      val outputVar = `var`(classOf[Output].asType, kryoVar.nextLocal)
      val objVar = `var`(classOf[AnyRef].asType, outputVar.nextLocal)
      thisVar.push().invokeV("write", kryoVar.push(), outputVar.push(), objVar.push().cast(classOf[BranchKey].asType))
      `return`()
    }

    methodDef.newMethod("write", Seq(classOf[Kryo].asType, classOf[Output].asType, classOf[BranchKey].asType)) { mb =>
      import mb._
      val kryoVar = `var`(classOf[Kryo].asType, thisVar.nextLocal)
      val outputVar = `var`(classOf[Output].asType, kryoVar.nextLocal)
      val branchKeyVar = `var`(classOf[BranchKey].asType, outputVar.nextLocal)
      outputVar.push().invokeV("writeInt", branchKeyVar.push().invokeV("id", Type.INT_TYPE))
      `return`()
    }

    methodDef.newMethod("read", classOf[AnyRef].asType,
      Seq(classOf[Kryo].asType, classOf[Input].asType, classOf[Class[_]].asType)) { mb =>
        import mb._
        val kryoVar = `var`(classOf[Kryo].asType, thisVar.nextLocal)
        val inputVar = `var`(classOf[Input].asType, kryoVar.nextLocal)
        val classVar = `var`(classOf[Class[_]].asType, inputVar.nextLocal)
        `return`(
          thisVar.push().invokeV("read", classOf[BranchKey].asType, kryoVar.push(), inputVar.push(), classVar.push()))
      }

    methodDef.newMethod("read", classOf[BranchKey].asType,
      Seq(classOf[Kryo].asType, classOf[Input].asType, classOf[Class[_]].asType)) { mb =>
        import mb._
        val kryoVar = `var`(classOf[Kryo].asType, thisVar.nextLocal)
        val inputVar = `var`(classOf[Input].asType, kryoVar.nextLocal)
        val classVar = `var`(classOf[Class[_]].asType, inputVar.nextLocal)
        `return`(
          invokeStatic(branchKeysType, "valueOf", classOf[BranchKey].asType,
            inputVar.push().invokeV("readInt", Type.INT_TYPE)))
      }
  }
}
