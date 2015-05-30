package com.asakusafw.spark.compiler
package subplan

import org.apache.hadoop.io.Writable
import org.objectweb.asm.{ Opcodes, Type }

import com.asakusafw.lang.compiler.api.JobflowProcessor.{ Context => JPContext }
import com.asakusafw.lang.compiler.model.graph.UserOperator
import com.asakusafw.lang.compiler.planning.SubPlan
import com.asakusafw.spark.compiler.operator.OperatorInfo
import com.asakusafw.spark.compiler.planning.SubPlanOutputInfo
import com.asakusafw.spark.runtime.rdd.BranchKey
import com.asakusafw.spark.runtime.io.{ BufferSlice, WritableBuffer }
import com.asakusafw.spark.tools.asm._

trait Serializing extends ClassBuilder {

  def jpContext: JPContext

  def branchKeys: BranchKeys

  def subplanOutputs: Seq[SubPlan.Output]

  override def defFields(fieldDef: FieldDef): Unit = {
    super.defFields(fieldDef)

    fieldDef.newField(
      Opcodes.ACC_PRIVATE | Opcodes.ACC_TRANSIENT,
      "buffer",
      classOf[WritableBuffer].asType)

    for {
      (output, i) <- subplanOutputs.zipWithIndex
    } {
      fieldDef.newField(
        Opcodes.ACC_PRIVATE | Opcodes.ACC_TRANSIENT,
        s"value${i}",
        outputType(output))
    }
  }

  override def defMethods(methodDef: MethodDef): Unit = {
    super.defMethods(methodDef)

    methodDef.newMethod("serialize", classOf[BufferSlice].asType,
      Seq(classOf[BranchKey].asType, classOf[AnyRef].asType)) { mb =>
        import mb._
        val branchVar = `var`(classOf[BranchKey].asType, thisVar.nextLocal)
        val valueVar = `var`(classOf[AnyRef].asType, branchVar.nextLocal)
        `return`(
          thisVar.push()
            .invokeV("serialize", classOf[BufferSlice].asType,
              branchVar.push(),
              valueVar.push().cast(classOf[Writable].asType)))
      }

    methodDef.newMethod("serialize", classOf[BufferSlice].asType,
      Seq(classOf[BranchKey].asType, classOf[Writable].asType)) { mb =>
        import mb._
        val branchVar = `var`(classOf[BranchKey].asType, thisVar.nextLocal)
        val valueVar = `var`(classOf[Writable].asType, branchVar.nextLocal)
        `return`(
          thisVar.push()
            .invokeV("buffer", classOf[WritableBuffer].asType)
            .invokeV("putAndSlice", classOf[BufferSlice].asType, valueVar.push()))
      }

    methodDef.newMethod("deserialize", classOf[AnyRef].asType,
      Seq(classOf[BranchKey].asType, classOf[BufferSlice].asType)) { mb =>
        import mb._
        val branchVar = `var`(classOf[BranchKey].asType, thisVar.nextLocal)
        val valueVar = `var`(classOf[BufferSlice].asType, branchVar.nextLocal)
        `return`(
          thisVar.push()
            .invokeV("deserialize", classOf[Writable].asType,
              branchVar.push(),
              valueVar.push().cast(classOf[BufferSlice].asType)))
      }

    methodDef.newMethod("deserialize", classOf[Writable].asType,
      Seq(classOf[BranchKey].asType, classOf[BufferSlice].asType)) { mb =>
        import mb._
        val branchVar = `var`(classOf[BranchKey].asType, thisVar.nextLocal)
        val sliceVar = `var`(classOf[BufferSlice].asType, branchVar.nextLocal)
        val valueVar =
          thisVar.push().invokeV("value", classOf[Writable].asType, branchVar.push())
            .store(sliceVar.nextLocal)
        thisVar.push().invokeV("buffer", classOf[WritableBuffer].asType)
          .invokeV(
            "resetAndGet",
            classOf[WritableBuffer].asType,
            sliceVar.push(),
            valueVar.push())
          .pop()
        `return`(valueVar.push())
      }

    methodDef.newMethod("buffer", classOf[WritableBuffer].asType, Seq.empty) { mb =>
      import mb._
      thisVar.push().getField("buffer", classOf[WritableBuffer].asType).unlessNotNull {
        thisVar.push().putField("buffer", classOf[WritableBuffer].asType,
          pushNew0(classOf[WritableBuffer].asType))
      }
      `return`(thisVar.push().getField("buffer", classOf[WritableBuffer].asType))
    }

    methodDef.newMethod("value", classOf[Writable].asType, Seq(classOf[BranchKey].asType)) { mb =>
      import mb._
      val branchVar = `var`(classOf[BranchKey].asType, thisVar.nextLocal)
      for {
        (output, i) <- subplanOutputs.zipWithIndex
      } {
        branchVar.push().unlessNotEqual(branchKeys.getField(mb, output.getOperator)) {
          val t = outputType(output)
          thisVar.push().getField(s"value${i}", t).unlessNotNull {
            thisVar.push().putField(s"value${i}", t, pushNew0(t))
          }
          `return`(thisVar.push().getField(s"value${i}", t))
        }
      }
      `throw`(pushNew0(classOf[AssertionError].asType))
    }
  }

  private def outputType(output: SubPlan.Output): Type = {
    val outputInfo = output.getAttribute(classOf[SubPlanOutputInfo])
    if (outputInfo.getOutputType == SubPlanOutputInfo.OutputType.AGGREGATED) {
      val op = outputInfo.getAggregationInfo.asInstanceOf[UserOperator]
      val operatorInfo = new OperatorInfo(op)(jpContext)
      import operatorInfo._
      assert(outputs.size == 1)
      outputs.head.getDataType.asType
    } else {
      output.getOperator.getDataType.asType
    }
  }
}
