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
package subplan

import org.apache.hadoop.io.Writable
import org.objectweb.asm.{ Opcodes, Type }

import com.asakusafw.lang.compiler.model.graph.UserOperator
import com.asakusafw.lang.compiler.planning.SubPlan
import com.asakusafw.spark.compiler.planning.SubPlanOutputInfo
import com.asakusafw.spark.runtime.rdd.BranchKey
import com.asakusafw.spark.runtime.io.WritableSerDe
import com.asakusafw.spark.tools.asm._

trait Serializing
  extends ClassBuilder
  with ScalaIdioms {

  implicit def context: SparkClientCompiler.Context

  def subplanOutputs: Seq[SubPlan.Output]

  override def defFields(fieldDef: FieldDef): Unit = {
    super.defFields(fieldDef)

    for {
      (output, i) <- subplanOutputs.zipWithIndex
      outputInfo = output.getAttribute(classOf[SubPlanOutputInfo])
      if outputInfo.getOutputType != SubPlanOutputInfo.OutputType.BROADCAST
    } {
      fieldDef.newField(
        Opcodes.ACC_PRIVATE | Opcodes.ACC_TRANSIENT,
        s"value${i}",
        outputType(output))
    }
  }

  override def defMethods(methodDef: MethodDef): Unit = {
    super.defMethods(methodDef)

    methodDef.newMethod("serialize", classOf[Array[Byte]].asType,
      Seq(classOf[BranchKey].asType, classOf[AnyRef].asType)) { mb =>
        import mb._ // scalastyle:ignore
        val branchVar = `var`(classOf[BranchKey].asType, thisVar.nextLocal)
        val valueVar = `var`(classOf[AnyRef].asType, branchVar.nextLocal)
        `return`(
          thisVar.push()
            .invokeV("serialize", classOf[Array[Byte]].asType,
              branchVar.push(),
              valueVar.push().cast(classOf[Writable].asType)))
      }

    methodDef.newMethod("serialize", classOf[Array[Byte]].asType,
      Seq(classOf[BranchKey].asType, classOf[Writable].asType)) { mb =>
        import mb._ // scalastyle:ignore
        val branchVar = `var`(classOf[BranchKey].asType, thisVar.nextLocal)
        val valueVar = `var`(classOf[Writable].asType, branchVar.nextLocal)
        `return`(
          pushObject(mb)(WritableSerDe)
            .invokeV("serialize", classOf[Array[Byte]].asType, valueVar.push()))
      }

    methodDef.newMethod("deserialize", classOf[AnyRef].asType,
      Seq(classOf[BranchKey].asType, classOf[Array[Byte]].asType)) { mb =>
        import mb._ // scalastyle:ignore
        val branchVar = `var`(classOf[BranchKey].asType, thisVar.nextLocal)
        val valueVar = `var`(classOf[Array[Byte]].asType, branchVar.nextLocal)
        `return`(
          thisVar.push()
            .invokeV("deserialize", classOf[Writable].asType,
              branchVar.push(),
              valueVar.push()))
      }

    methodDef.newMethod("deserialize", classOf[Writable].asType,
      Seq(classOf[BranchKey].asType, classOf[Array[Byte]].asType)) { mb =>
        import mb._ // scalastyle:ignore
        val branchVar = `var`(classOf[BranchKey].asType, thisVar.nextLocal)
        val sliceVar = `var`(classOf[Array[Byte]].asType, branchVar.nextLocal)
        val valueVar =
          thisVar.push().invokeV("value", classOf[Writable].asType, branchVar.push())
            .store(sliceVar.nextLocal)
        pushObject(mb)(WritableSerDe)
          .invokeV(
            "deserialize",
            sliceVar.push(),
            valueVar.push())
        `return`(valueVar.push())
      }

    methodDef.newMethod("value", classOf[Writable].asType, Seq(classOf[BranchKey].asType)) { mb =>
      import mb._ // scalastyle:ignore
      val branchVar = `var`(classOf[BranchKey].asType, thisVar.nextLocal)
      for {
        (output, i) <- subplanOutputs.zipWithIndex
      } {
        branchVar.push().unlessNotEqual(context.branchKeys.getField(mb, output.getOperator)) {
          val t = outputType(output)
          val outputInfo = output.getAttribute(classOf[SubPlanOutputInfo])
          if (outputInfo.getOutputType == SubPlanOutputInfo.OutputType.BROADCAST) {
            `return`(pushNew0(t))
          } else {
            thisVar.push().getField(s"value${i}", t).unlessNotNull {
              thisVar.push().putField(s"value${i}", t, pushNew0(t))
            }
            `return`(thisVar.push().getField(s"value${i}", t))
          }
        }
      }
      `throw`(pushNew0(classOf[AssertionError].asType))
    }
  }

  private def outputType(output: SubPlan.Output): Type = {
    val outputInfo = output.getAttribute(classOf[SubPlanOutputInfo])
    if (outputInfo.getOutputType == SubPlanOutputInfo.OutputType.AGGREGATED) {
      val op = outputInfo.getAggregationInfo.asInstanceOf[UserOperator]
      assert(op.outputs.size == 1)
      op.outputs.head.getDataType.asType
    } else {
      output.getOperator.getDataType.asType
    }
  }
}
