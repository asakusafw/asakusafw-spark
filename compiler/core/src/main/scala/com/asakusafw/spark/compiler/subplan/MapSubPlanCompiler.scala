package com.asakusafw.spark.compiler
package subplan

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.reflect.NameTransformer

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.objectweb.asm.Type

import com.asakusafw.lang.compiler.model.graph._
import com.asakusafw.lang.compiler.planning.SubPlan
import com.asakusafw.lang.compiler.planning.spark.DominantOperator
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.spark.compiler.operator._
import com.asakusafw.spark.compiler.spi.{ OperatorCompiler, OperatorType, SubPlanCompiler }
import com.asakusafw.spark.runtime.fragment._
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._
import com.asakusafw.vocabulary.operator._

class MapSubPlanCompiler extends SubPlanCompiler {

  import MapSubPlanCompiler._

  override def support(operator: Operator)(implicit context: Context): Boolean = {
    OperatorCompiler.support(
      operator,
      OperatorType.MapType)(
        OperatorCompiler.Context(context.flowId, context.jpContext))
  }

  override def instantiator: Instantiator = MapSubPlanCompiler.MapDriverInstantiator

  override def compile(subplan: SubPlan)(implicit context: Context): Type = {
    val operator = subplan.getAttribute(classOf[DominantOperator]).getDominantOperator

    val builder = new MapDriverClassBuilder(context.flowId, operator.getInputs.head.getDataType.asType) {

      override val jpContext = context.jpContext

      override val dominantOperator = operator

      override val subplanOutputs: Seq[SubPlan.Output] = subplan.getOutputs.toSeq

      override def defMethods(methodDef: MethodDef): Unit = {
        super.defMethods(methodDef)

        methodDef.newMethod("fragments", classOf[(_, _)].asType, Seq.empty) { mb =>
          import mb._
          val nextLocal = new AtomicInteger(thisVar.nextLocal)

          val fragmentBuilder = new FragmentTreeBuilder(
            mb, nextLocal)(OperatorCompiler.Context(context.flowId, context.jpContext))
          val fragmentVar = fragmentBuilder.build(operator)
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

object MapSubPlanCompiler {

  val CompilableOperators: Set[Class[_]] =
    Set(classOf[Branch], classOf[Convert], classOf[Extract], classOf[Update])

  object MapDriverInstantiator extends Instantiator {

    override def newInstance(
      driverType: Type,
      subplan: SubPlan)(implicit context: Context): Var = {
      import context.mb._

      val broadcastsVar = {
        // TODO broadcast
        val builder = getStatic(Map.getClass.asType, "MODULE$", Map.getClass.asType)
          .invokeV("newBuilder", classOf[mutable.Builder[_, _]].asType)
        val broadcasts = builder.invokeI("result", classOf[AnyRef].asType).cast(classOf[Map[_, _]].asType)
        broadcasts.store(context.nextLocal.getAndAdd(broadcasts.size))
      }

      val prevRddVars = subplan.getInputs.toSet[SubPlan.Input]
        .flatMap(input => input.getOpposites.toSet[SubPlan.Output])
        .map(_.getOperator.getSerialNumber)
        .map(context.rddVars)

      val mapDriver = pushNew(driverType)
      mapDriver.dup().invokeInit(
        context.scVar.push(),
        context.hadoopConfVar.push(),
        broadcastsVar.push(), {
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
      mapDriver.store(context.nextLocal.getAndAdd(mapDriver.size))
    }
  }
}
