package com.asakusafw.spark.compiler
package subplan

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.reflect.NameTransformer

import org.objectweb.asm.Type

import com.asakusafw.lang.compiler.api.JobflowProcessor.{ Context => JPContext }
import com.asakusafw.lang.compiler.model.graph._
import com.asakusafw.spark.compiler.operator.{ EdgeFragmentClassBuilder, OutputFragmentClassBuilder }
import com.asakusafw.spark.compiler.spi.{ OperatorCompiler, OperatorType }
import com.asakusafw.spark.runtime.fragment._
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._

class FragmentTreeBuilder(
    mb: MethodBuilder,
    nextLocal: AtomicInteger)(implicit context: OperatorCompiler.Context) {
  import mb._

  val operatorFragmentTypes: mutable.Map[Long, Type] = mutable.Map.empty
  val edgeFragmentTypes: mutable.Map[Type, Type] = mutable.Map.empty

  val vars: mutable.Map[Long, Var] = mutable.Map.empty
  val outputVars: mutable.Map[Long, Var] = mutable.Map.empty

  def build(operator: Operator): Var = {
    val t = operatorFragmentTypes.getOrElseUpdate(
      operator.getOriginalSerialNumber, {
        operator match {
          case marker: MarkerOperator =>
            OutputFragmentClassBuilder.getOrCompile(context.flowId, marker.getInput.getDataType.asType, context.jpContext)
          case operator =>
            OperatorCompiler.compile(operator, OperatorType.MapType)
        }
      })
    operator match {
      case marker: MarkerOperator =>
        val fragment = pushNew0(t)
        val fragmentVar = fragment.store(nextLocal.getAndAdd(fragment.size))
        outputVars += (marker.getOriginalSerialNumber -> fragmentVar)
        fragmentVar
      case _ =>
        val outputs = operator.getOutputs.map(build)
        val fragment = pushNew(t)
        fragment.dup().invokeInit(outputs.map(_.push().asType(classOf[Fragment[_]].asType)): _*)
        fragment.store(nextLocal.getAndAdd(fragment.size))
    }
  }

  def build(output: OperatorOutput): Var = {
    if (output.getOpposites.size == 0) {
      vars.getOrElseUpdate(-1L, {
        val fragment = getStatic(StopFragment.getClass.asType, "MODULE$", StopFragment.getClass.asType)
        fragment.store(nextLocal.getAndAdd(fragment.size))
      })
    } else if (output.getOpposites.size > 1) {
      val opposites = output.getOpposites.toSeq.map(_.getOwner).map { operator =>
        vars.getOrElseUpdate(operator.getOriginalSerialNumber, build(operator))
      }
      val fragment = pushNew(
        edgeFragmentTypes.getOrElseUpdate(
          output.getDataType.asType, {
            EdgeFragmentClassBuilder.getOrCompile(context.flowId, output.getDataType.asType, context.jpContext)
          }))
      fragment.dup().invokeInit({
        val builder = getStatic(Seq.getClass.asType, "MODULE$", Seq.getClass.asType)
          .invokeV("newBuilder", classOf[mutable.Builder[_, _]].asType)
        opposites.foreach { opposite =>
          builder.invokeI(NameTransformer.encode("+="),
            classOf[mutable.Builder[_, _]].asType,
            opposite.push().asType(classOf[AnyRef].asType))
        }
        builder.invokeI("result", classOf[AnyRef].asType).cast(classOf[Seq[_]].asType)
      })
      fragment.store(nextLocal.getAndAdd(fragment.size))
    } else {
      val operator = output.getOpposites.head.getOwner
      vars.getOrElseUpdate(operator.getOriginalSerialNumber, build(operator))
    }
  }

  def buildOutputsVar(): Var = {
    val builder = getStatic(Map.getClass.asType, "MODULE$", Map.getClass.asType)
      .invokeV("newBuilder", classOf[mutable.Builder[_, _]].asType)
    outputVars.toSeq.sortBy(_._1).foreach {
      case (sn, outputVar) =>
        builder.invokeI(NameTransformer.encode("+="),
          classOf[mutable.Builder[_, _]].asType,
          getStatic(Tuple2.getClass.asType, "MODULE$", Tuple2.getClass.asType).
            invokeV("apply", classOf[(_, _)].asType,
              ldc(sn).box().asType(classOf[AnyRef].asType),
              outputVar.push().asType(classOf[AnyRef].asType))
            .asType(classOf[AnyRef].asType))
    }
    val map = builder.invokeI("result", classOf[AnyRef].asType).cast(classOf[Map[_, _]].asType)
    map.store(nextLocal.getAndAdd(map.size))
  }
}
