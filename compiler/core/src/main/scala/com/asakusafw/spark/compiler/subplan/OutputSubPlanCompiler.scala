package com.asakusafw.spark.compiler
package subplan

import scala.collection.JavaConversions._

import org.objectweb.asm.Type

import com.asakusafw.lang.compiler.model.graph._
import com.asakusafw.lang.compiler.planning.SubPlan
import com.asakusafw.lang.compiler.planning.spark.DominantOperator
import com.asakusafw.spark.compiler.spi.SubPlanCompiler
import com.asakusafw.spark.tools.asm._

class OutputSubPlanCompiler extends SubPlanCompiler {

  override def of(operator: Operator, classLoader: ClassLoader): Boolean = {
    operator.isInstanceOf[ExternalOutput]
  }

  override def instantiator: Instantiator = ???

  override def compile(subplan: SubPlan)(implicit context: Context): Type = {
    val dominant = subplan.getAttribute(classOf[DominantOperator]).getDominantOperator
    assert(dominant.isInstanceOf[ExternalOutput])
    val operator = dominant.asInstanceOf[ExternalOutput]
    val outputPath = s"${operator.getName}/${operator.getSerialNumber}"

    context.jpContext.addExternalOutput(
      operator.getName, operator.getInfo,
      Seq(context.jpContext.getOptions.getRuntimeWorkingPath(s"${outputPath}/part-*")))

    val builder = new OutputDriverClassBuilder(context.flowId, operator.getDataType.asType) {

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
