package com.asakusafw.spark.compiler
package subplan

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.reflect.NameTransformer

import org.objectweb.asm.Type

import com.asakusafw.lang.compiler.model.graph._
import com.asakusafw.lang.compiler.planning.SubPlan
import com.asakusafw.lang.compiler.planning.spark.DominantOperator
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.spark.compiler.operator._
import com.asakusafw.spark.compiler.spi.SubPlanCompiler
import com.asakusafw.spark.runtime.fragment._
import com.asakusafw.spark.tools.asm._

class InputSubPlanCompiler extends SubPlanCompiler {

  override def of(operator: Operator, classLoader: ClassLoader): Boolean = {
    operator.isInstanceOf[ExternalInput]
  }

  override def instantiator: Instantiator = ???

  override def compile(subplan: SubPlan)(implicit context: Context): Type = {
    val dominant = subplan.getAttribute(classOf[DominantOperator]).getDominantOperator
    assert(dominant.isInstanceOf[ExternalInput])
    val operator = dominant.asInstanceOf[ExternalInput]
    val inputRef = context.jpContext.addExternalInput(operator.getName, operator.getInfo)

    val outputs = subplan.getOutputs.toSet[SubPlan.Output].map(_.getOperator)

    val builder = new InputDriverClassBuilder(context.flowId, operator.getDataType.asType) {

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
