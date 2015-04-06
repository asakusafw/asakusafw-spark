package com.asakusafw.spark.compiler
package subplan

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.reflect.{ ClassTag, NameTransformer }

import org.apache.spark.{ HashPartitioner, Partitioner }
import org.apache.spark.broadcast.Broadcast
import org.objectweb.asm.Type

import com.asakusafw.lang.compiler.model.graph._
import com.asakusafw.lang.compiler.planning.{ PlanMarker, SubPlan }
import com.asakusafw.lang.compiler.planning.spark.{ DominantOperator, PartitioningParameters }
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.spark.compiler.operator._
import com.asakusafw.spark.compiler.ordering.OrderingClassBuilder
import com.asakusafw.spark.compiler.spi.{ OperatorCompiler, OperatorType, SubPlanCompiler }
import com.asakusafw.spark.runtime.fragment._
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._

class InputSubPlanCompiler extends SubPlanCompiler {

  override def support(operator: Operator)(implicit context: Context): Boolean = {
    operator.isInstanceOf[ExternalInput]
  }

  override def instantiator: Instantiator = InputSubPlanCompiler.InputDriverInstantiator

  override def compile(subplan: SubPlan)(implicit context: Context): Type = {
    val dominant = subplan.getAttribute(classOf[DominantOperator]).getDominantOperator
    assert(dominant.isInstanceOf[ExternalInput])
    val operator = dominant.asInstanceOf[ExternalInput]
    val inputRef = context.externalInputs.getOrElseUpdate(
      operator.getName,
      context.jpContext.addExternalInput(operator.getName, operator.getInfo))

    val builder = new InputDriverClassBuilder(context.flowId, operator.getDataType.asType) {

      override val jpContext = context.jpContext

      override val shuffleKeyTypes = context.shuffleKeyTypes

      override val dominantOperator = operator

      override val subplanOutputs: Seq[SubPlan.Output] = subplan.getOutputs.toSeq

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

        methodDef.newMethod("fragments", classOf[(_, _)].asType, Seq.empty) { mb =>
          import mb._
          val nextLocal = new AtomicInteger(thisVar.nextLocal)

          val fragmentBuilder = new FragmentTreeBuilder(
            mb, nextLocal)(OperatorCompiler.Context(context.flowId, context.jpContext, context.shuffleKeyTypes))
          val fragmentVar = fragmentBuilder.build(operator.getOperatorPort)
          val outputsVar = fragmentBuilder.buildOutputsVar(subplanOutputs)

          `return`(
            getStatic(Tuple2.getClass.asType, "MODULE$", Tuple2.getClass.asType).
              invokeV("apply", classOf[(_, _)].asType,
                fragmentVar.push().asType(classOf[AnyRef].asType), outputsVar.push().asType(classOf[AnyRef].asType)))
        }

        defName(methodDef)
      }
    }

    context.jpContext.addClass(builder)
  }
}

object InputSubPlanCompiler {

  object InputDriverInstantiator extends Instantiator {

    override def newInstance(
      driverType: Type,
      subplan: SubPlan)(implicit context: Context): Var = {
      import context.mb._

      val inputDriver = pushNew(driverType)
      inputDriver.dup().invokeInit(
        context.scVar.push(),
        context.hadoopConfVar.push(),
        context.broadcastsVar.push())
      inputDriver.store(context.nextLocal.getAndAdd(inputDriver.size))
    }
  }
}
