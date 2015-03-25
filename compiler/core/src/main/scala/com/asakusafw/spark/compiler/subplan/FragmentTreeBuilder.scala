package com.asakusafw.spark.compiler
package subplan

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.reflect.NameTransformer

import org.objectweb.asm.Type

import com.asakusafw.lang.compiler.model.graph._
import com.asakusafw.spark.runtime.fragment._
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._

class FragmentTreeBuilder(
    mb: MethodBuilder,
    operatorFragmentTypes: Map[Long, Type],
    edgeFragmentTypes: Map[Type, Type],
    nextLocal: AtomicInteger) {
  import mb._

  val vars: mutable.Map[Long, Var] = mutable.Map.empty

  def build(operator: Operator): Var = {
    val t = operatorFragmentTypes(operator.getOriginalSerialNumber)
    val fragment = operator match {
      case marker: MarkerOperator =>
        pushNew0(t)
      case _ =>
        val outputs = operator.getOutputs.map(build)
        val fragment = pushNew(t)
        fragment.dup().invokeInit(outputs.map(_.push().asType(classOf[Fragment[_]].asType)): _*)
        fragment
    }
    fragment.store(nextLocal.getAndAdd(fragment.size))
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
      val fragment = pushNew(edgeFragmentTypes(output.getDataType.asType))
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
}
