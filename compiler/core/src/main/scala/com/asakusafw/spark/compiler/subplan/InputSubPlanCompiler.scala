package com.asakusafw.spark.compiler
package subplan

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.reflect.NameTransformer

import org.objectweb.asm.Type

import com.asakusafw.lang.compiler.model.graph._
import com.asakusafw.lang.compiler.planning.SubPlan
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.spark.compiler.operator._
import com.asakusafw.spark.compiler.spi.SubPlanCompiler
import com.asakusafw.spark.runtime.fragment._
import com.asakusafw.spark.tools.asm._

class InputSubPlanCompiler extends SubPlanCompiler {

  def of: SubPlanType = SubPlanType.InputSubPlan

  def compile(subplan: SubPlan)(implicit context: Context): Type = {
    val inputs = subplan.getInputs.toSet[SubPlan.Input].map(_.getOperator)
    assert(inputs.size == 1)
    val heads = inputs.flatMap(_.getOutput.getOpposites.map(_.getOwner))
    assert(heads.size == 1)
    assert(heads.head.isInstanceOf[ExternalInput])
    val input = heads.head.asInstanceOf[ExternalInput]
    val inputRef = context.jpContext.addExternalInput(input.getName, input.getInfo)

    val outputs = subplan.getOutputs.toSet[SubPlan.Output].map(_.getOperator)
    assert(outputs.size == 1)
    val lasts = outputs.flatMap(_.getInput.getOpposites.map(_.getOwner))
    assert(lasts.size == 1)
    assert(lasts.head == input)

    val builder = new InputDriverClassBuilder(context.flowId, input.getDataType.asType) {

      override def defMethods(methodDef: MethodDef): Unit = {
        super.defMethods(methodDef)

        methodDef.newMethod("paths", classOf[Set[String]].asType, Seq.empty) { mb =>
          import mb._
          val builder = getStatic(Set.getClass.asType, "MODULE$", Set.getClass.asType)
            .invokeV("newBuilder", classOf[mutable.Builder[_, _]].asType)
          inputRef.getPaths.toSeq.sorted.foreach { path =>
            builder.invokeI(NameTransformer.encode("+="),
              classOf[mutable.Builder[_, _]].asType, ldc(path).asType(classOf[AnyRef].asType))
          }
          `return`(builder.invokeI("result", classOf[AnyRef].asType).cast(classOf[Set[_]].asType))
        }

        methodDef.newMethod("branchKey", classOf[AnyRef].asType, Seq.empty) { mb =>
          import mb._
          `return`(thisVar.push().invokeV("branchKey", Type.LONG_TYPE).box().asType(classOf[AnyRef].asType))
        }

        methodDef.newMethod("branchKey", Type.LONG_TYPE, Seq.empty) { mb =>
          import mb._
          `return`(ldc(outputs.head.getOriginalSerialNumber))
        }
      }
    }

    context.jpContext.addClass(builder)
  }
}
