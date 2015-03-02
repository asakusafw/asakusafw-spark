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
import com.asakusafw.spark.tools.asm.MethodBuilder._

class CoGroupSubPlanCompiler extends SubPlanCompiler {

  def of: SubPlanType = SubPlanType.CoGroupSubPlan

  def compile(subplan: SubPlan)(implicit context: Context): (Type, Array[Byte]) = {
    val inputs = subplan.getInputs.toSet[SubPlan.Input].map(_.getOperator)
    val heads = inputs.flatMap(_.getOutput.getOpposites.map(_.getOwner))
    assert(heads.size == 1)
    assert(heads.head.isInstanceOf[UserOperator])
    val cogroup = heads.head.asInstanceOf[UserOperator]

    val outputs = subplan.getOutputs.toSet[SubPlan.Output].map(_.getOperator).toSeq

    implicit val compilerContext = OperatorCompiler.Context(context.jpContext)
    val operators = subplan.getOperators.map { operator =>
      operator -> OperatorCompiler.compile(operator)
    }.toMap
    context.fragments ++= operators.values

    val edges = subplan.getOperators.flatMap {
      _.getOutputs.collect {
        case output if output.getOpposites.size > 1 => output.getDataType.asType
      }
    }.map { dataType =>
      val builder = new EdgeFragmentClassBuilder(dataType)
      dataType -> (builder.thisType, builder.build())
    }.toMap
    context.fragments ++= edges.values

    val builder = new CoGroupDriverClassBuilder(Type.LONG_TYPE, classOf[AnyRef].asType) {

      override def outputMarkers: Seq[MarkerOperator] = outputs

      override def defMethods(methodDef: MethodDef): Unit = {
        super.defMethods(methodDef)

        methodDef.newMethod("fragments", classOf[(_, _)].asType, Seq.empty) { mb =>
          import mb._
          val nextLocal = new AtomicInteger(thisVar.nextLocal)

          val fragmentBuilder = new FragmentTreeBuilder(
            mb,
            operators.map {
              case (operator, (t, _)) => operator -> t
            },
            edges.map {
              case (dataType, (t, _)) => dataType -> t
            },
            nextLocal)
          val fragmentVar = fragmentBuilder.build(cogroup)

          val outputsVar = {
            val builder = getStatic(Map.getClass.asType, "MODULE$", Map.getClass.asType)
              .invokeV("newBuilder", classOf[mutable.Builder[_, _]].asType)
            outputs.sortBy(_.getOriginalSerialNumber).foreach { op =>
              builder.invokeI(NameTransformer.encode("+="),
                classOf[mutable.Builder[_, _]].asType,
                getStatic(Tuple2.getClass.asType, "MODULE$", Tuple2.getClass.asType).
                  invokeV("apply", classOf[(_, _)].asType,
                    ldc(op.getOriginalSerialNumber).box().asType(classOf[AnyRef].asType),
                    fragmentBuilder.vars(op.getOriginalSerialNumber).push().asType(classOf[AnyRef].asType))
                  .asType(classOf[AnyRef].asType))
            }
            val map = builder.invokeI("result", classOf[AnyRef].asType).cast(classOf[Map[_, _]].asType)
            map.store(nextLocal.getAndAdd(map.size))
          }

          `return`(
            getStatic(Tuple2.getClass.asType, "MODULE$", Tuple2.getClass.asType).
              invokeV("apply", classOf[(_, _)].asType,
                fragmentVar.push().asType(classOf[AnyRef].asType), outputsVar.push().asType(classOf[AnyRef].asType)))
        }
      }
    }
    (builder.thisType, builder.build())
  }
}
