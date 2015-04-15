package com.asakusafw.spark.compiler
package serializer

import com.esotericsoftware.kryo.{ Kryo, Registration, Serializer }

import org.objectweb.asm.Type

import com.asakusafw.lang.compiler.api.JobflowProcessor.{ Context => JPContext }
import com.asakusafw.spark.runtime.driver.{ BranchKey, BroadcastId }
import com.asakusafw.spark.runtime.serializer.KryoRegistrator
import com.asakusafw.spark.tools.asm._

object KryoRegistratorCompiler {

  case class Context(flowId: String, jpContext: JPContext)

  def compile(
    writables: Set[Type],
    branchKeySerializer: Type,
    broadcastIdSerializer: Type)(implicit context: Context): Type = {
    val serializers = writables.map { writable =>
      writable -> WritableSerializerClassBuilder.getOrCompile(context.flowId, writable, context.jpContext)
    }

    val builder = new ClassBuilder(
      Type.getType(s"L${GeneratedClassPackageInternalName}/${context.flowId}/serializer/KryoRegistrator;"),
      classOf[KryoRegistrator].asType) {

      override def defMethods(methodDef: MethodDef): Unit = {
        super.defMethods(methodDef)

        methodDef.newMethod("registerClasses", Seq(classOf[Kryo].asType)) { mb =>
          import mb._
          val kryoVar = `var`(classOf[Kryo].asType, thisVar.nextLocal)
          thisVar.push().invokeS(classOf[KryoRegistrator].asType, "registerClasses", kryoVar.push())

          serializers.foreach {
            case (dataModelType, serializerType) =>
              kryoVar.push().invokeV(
                "register",
                classOf[Registration].asType,
                ldc(dataModelType).asType(classOf[Class[_]].asType),
                pushNew0(serializerType).asType(classOf[Serializer[_]].asType)).pop()
          }

          kryoVar.push().invokeV(
            "register",
            classOf[Registration].asType,
            ldc(classOf[BranchKey].asType).asType(classOf[Class[_]].asType),
            pushNew0(branchKeySerializer).asType(classOf[Serializer[_]].asType)).pop()

          kryoVar.push().invokeV(
            "register",
            classOf[Registration].asType,
            ldc(classOf[BroadcastId].asType).asType(classOf[Class[_]].asType),
            pushNew0(broadcastIdSerializer).asType(classOf[Serializer[_]].asType)).pop()

          `return`()
        }
      }
    }

    context.jpContext.addClass(builder)
  }
}
