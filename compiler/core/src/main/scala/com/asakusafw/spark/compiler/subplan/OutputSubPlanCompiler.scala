package com.asakusafw.spark.compiler
package subplan

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.reflect.NameTransformer

import org.apache.spark.rdd.RDD
import org.objectweb.asm.Type

import com.asakusafw.lang.compiler.model.graph._
import com.asakusafw.lang.compiler.planning.SubPlan
import com.asakusafw.lang.compiler.planning.spark.DominantOperator
import com.asakusafw.spark.compiler.spi.SubPlanCompiler
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._

class OutputSubPlanCompiler extends SubPlanCompiler {

  override def of(operator: Operator, classLoader: ClassLoader): Boolean = {
    operator.isInstanceOf[ExternalOutput]
  }

  override def instantiator: Instantiator = OutputSubPlanCompiler.OutputDriverInstantiator

  override def compile(subplan: SubPlan)(implicit context: Context): Type = {
    val dominant = subplan.getAttribute(classOf[DominantOperator]).getDominantOperator
    assert(dominant.isInstanceOf[ExternalOutput])
    val operator = dominant.asInstanceOf[ExternalOutput]
    val outputPath = s"${operator.getName}/${operator.getSerialNumber}"

    context.jpContext.addExternalOutput(
      operator.getName, operator.getInfo,
      Seq(context.jpContext.getOptions.getRuntimeWorkingPath(s"${outputPath}/part-*")))

    val builder = new OutputDriverClassBuilder(context.flowId, operator.getDataType.asType) {

      override def dominantOperator = operator

      override def defMethods(methodDef: MethodDef): Unit = {
        super.defMethods(methodDef)

        methodDef.newMethod("path", classOf[String].asType, Seq.empty) { mb =>
          import mb._
          `return`(ldc(context.jpContext.getOptions.getRuntimeWorkingPath(outputPath)))
        }

        defName(methodDef)
      }
    }

    context.jpContext.addClass(builder)
  }
}

object OutputSubPlanCompiler {

  object OutputDriverInstantiator extends Instantiator {

    override def newInstance(
      driverType: Type,
      subplan: SubPlan)(implicit context: Context): Var = {
      import context.mb._
      val prevRddVars = subplan.getInputs.toSet[SubPlan.Input]
        .flatMap(input => input.getOpposites.toSet[SubPlan.Output])
        .map(_.getOperator.getSerialNumber)
        .map(context.rddVars)
      val outputDriver = pushNew(driverType)
      outputDriver.dup().invokeInit(
        context.scVar.push(), {
          val builder = getStatic(Seq.getClass.asType, "MODULE$", Seq.getClass.asType)
            .invokeV("newBuilder", classOf[mutable.Builder[_, _]].asType)
          prevRddVars.foreach { rddVar =>
            builder.invokeI(
              NameTransformer.encode("+="),
              classOf[mutable.Builder[_, _]].asType,
              rddVar.push().asType(classOf[AnyRef].asType))
          }
          builder.invokeI("result", classOf[AnyRef].asType).cast(classOf[Seq[_]].asType)
        })
      outputDriver.store(context.nextLocal.getAndAdd(outputDriver.size))
    }
  }
}
