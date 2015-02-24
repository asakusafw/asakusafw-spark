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
import com.asakusafw.vocabulary.operator.CoGroup

class CoGroupSubPlanCompiler extends SubPlanCompiler {

  def of: SubPlanType = SubPlanType.CoGroupSubPlan

  def compile(subplan: SubPlan)(implicit context: Context): ClassBuilder = {
    val inputs = subplan.getInputs.toSet[SubPlan.Input].map(_.getOperator)
    val heads = inputs.flatMap(_.getOutput.getOpposites.map(_.getOwner))
    assert(heads.size == 1)
    assert(heads.head.isInstanceOf[UserOperator])
    assert(heads.head.asInstanceOf[UserOperator]
      .getAnnotation.getDeclaringClass.resolve(context.jpContext.getClassLoader) == classOf[CoGroup])
    val cogroup = heads.head.asInstanceOf[UserOperator]

    val outputs = subplan.getOutputs.toSet[SubPlan.Output].map(_.getOperator).toSeq.sortBy(_.getOriginalSerialNumber)

    new CoGroupDriverClassBuilder(Type.LONG_TYPE, classOf[String].asType) {

      override def initBranchKeys(mb: MethodBuilder): Stack = {
        import mb._
        getStatic(Predef.getClass.asType, "MODULE$", Predef.getClass.asType)
          .invokeV("longArrayOps", classOf[mutable.ArrayOps[_]].asType, {
            val arr = pushNewArray(Type.LONG_TYPE, outputs.size)
            outputs.zipWithIndex.foreach {
              case (op, i) =>
                arr.dup().astore(ldc(i), ldc(op.getOriginalSerialNumber))
            }
            arr
          })
          .invokeI("toSet", classOf[Set[_]].asType)
      }

      override def initPartitioners(mb: MethodBuilder): Stack = {
        import mb._
        // TODO
        getStatic(Map.getClass.asType, "MODULE$", Map.getClass.asType)
          .invokeV("empty", classOf[Map[_, _]].asType)
      }

      override def initOrderings(mb: MethodBuilder): Stack = {
        import mb._
        // TODO
        getStatic(Map.getClass.asType, "MODULE$", Map.getClass.asType)
          .invokeV("empty", classOf[Map[_, _]].asType)
      }

      override def defMethods(methodDef: MethodDef): Unit = {
        super.defMethods(methodDef)

        methodDef.newMethod("fragments", classOf[(_, _)].asType, Seq.empty) { mb =>
          import mb._
          val nextLocal = new AtomicInteger(thisVar.nextLocal)

          val vars: mutable.Map[Long, Var] = mutable.Map.empty[Long, Var]
          def buildFragment(operator: Operator): Var = {
            val builder = OperatorCompiler.compile(operator)(OperatorCompiler.Context(context.jpContext))
            context.fragments += builder

            val outputs: Seq[Var] = operator.getOutputs.collect {
              case output if output.getOpposites.size > 1 =>
                val builder = new EdgeFragmentClassBuilder(output.getDataType.asType)
                context.fragments += builder

                val opposites = output.getOpposites.toSeq.map(_.getOwner).map(op =>
                  vars.getOrElseUpdate(op.getOriginalSerialNumber, buildFragment(op)))
                val fragment = pushNew(builder.thisType)
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
              case output if output.getOpposites.size == 1 =>
                val op = output.getOpposites.head.getOwner
                vars.getOrElseUpdate(op.getOriginalSerialNumber, buildFragment(op))
            }
            val fragment = pushNew(builder.thisType)
            fragment.dup().invokeInit(outputs.map(_.push().asType(classOf[Fragment[_]].asType)): _*)
            fragment.store(nextLocal.getAndAdd(fragment.size))
          }
          val fragmentVar = vars.getOrElseUpdate(cogroup.getOriginalSerialNumber, buildFragment(cogroup))

          val outputsVar = {
            val builder = getStatic(Map.getClass.asType, "MODULE$", Map.getClass.asType)
              .invokeV("newBuilder", classOf[mutable.Builder[_, _]].asType)
            outputs.foreach { op =>
              builder.invokeI(NameTransformer.encode("+="),
                classOf[mutable.Builder[_, _]].asType,
                getStatic(Tuple2.getClass.asType, "MODULE$", Tuple2.getClass.asType).
                  invokeV("apply", classOf[(_, _)].asType,
                    ldc(op.getOriginalSerialNumber).box().asType(classOf[AnyRef].asType),
                    vars(op.getOriginalSerialNumber).push().asType(classOf[AnyRef].asType))
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

        methodDef.newMethod("shuffleKey", classOf[DataModel[_]].asType,
          Seq(classOf[AnyRef].asType, classOf[DataModel[_]].asType)) { mb =>
            import mb._
            // TODO
            `return`(pushNull(classOf[DataModel[_]].asType))
          }
      }
    }
  }
}
