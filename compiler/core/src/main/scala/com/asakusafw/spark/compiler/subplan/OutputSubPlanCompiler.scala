package com.asakusafw.spark.compiler
package subplan

import scala.collection.JavaConversions._

import org.objectweb.asm.Type

import com.asakusafw.lang.compiler.model.graph.ExternalOutput
import com.asakusafw.lang.compiler.planning.SubPlan
import com.asakusafw.spark.compiler.spi.SubPlanCompiler
import com.asakusafw.spark.tools.asm._

class OutputSubPlanCompiler extends SubPlanCompiler {

  def of: SubPlanType = SubPlanType.OutputSubPlan

  def compile(subplan: SubPlan)(implicit context: Context): Type = {
    val inputs = subplan.getInputs.toSet[SubPlan.Input].map(_.getOperator)
    val heads = inputs.flatMap(_.getOutput.getOpposites.map(_.getOwner))
    assert(heads.size == 1)
    assert(heads.head.isInstanceOf[ExternalOutput])

    val output = heads.head.asInstanceOf[ExternalOutput]
    val outputPath = s"${output.getName}/${output.getSerialNumber}"

    val outputRef = context.jpContext.addExternalOutput(
      output.getName, output.getInfo,
      Seq(context.jpContext.getOptions.getRuntimeWorkingPath(s"${outputPath}/part-*")))

    val outputs = subplan.getOutputs.toSet[SubPlan.Output].map(_.getOperator).toSeq

    val builder = new OutputDriverClassBuilder(context.flowId, output.getDataType.asType) {

      override def defMethods(methodDef: MethodDef): Unit = {
        super.defMethods(methodDef)

        methodDef.newMethod("path", classOf[String].asType, Seq.empty) { mb =>
          import mb._
          `return`(ldc(context.jpContext.getOptions.getRuntimeWorkingPath(outputPath)))
        }
      }
    }

    context.jpContext.addClass(builder)
  }
}
