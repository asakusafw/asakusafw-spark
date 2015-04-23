package com.asakusafw.spark.compiler
package subplan

import scala.collection.mutable
import scala.collection.JavaConversions._
import scala.reflect.NameTransformer

import org.apache.spark.Partitioner
import org.objectweb.asm.{ Opcodes, Type }
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.lang.compiler.api.JobflowProcessor.{ Context => JPContext }
import com.asakusafw.lang.compiler.model.graph.Group
import com.asakusafw.lang.compiler.planning.SubPlan
import com.asakusafw.spark.compiler.planning.SubPlanOutputInfo
import com.asakusafw.spark.runtime.driver.ShuffleKey
import com.asakusafw.spark.runtime.rdd.BranchKey
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._

trait OrderingsField extends ClassBuilder {

  def flowId: String

  def jpContext: JPContext

  def branchKeys: BranchKeys

  def subplanOutputs: Seq[SubPlan.Output]

  def defOrderingsField(fieldDef: FieldDef): Unit = {
    fieldDef.newField(
      Opcodes.ACC_PRIVATE | Opcodes.ACC_TRANSIENT,
      "orderings",
      classOf[Map[_, _]].asType,
      new TypeSignatureBuilder()
        .newClassType(classOf[Map[_, _]].asType) {
          _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[BranchKey].asType)
            .newTypeArgument(SignatureVisitor.INSTANCEOF) {
              _.newClassType(classOf[Ordering[_]].asType) {
                _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[ShuffleKey].asType)
              }
            }
        }
        .build())
  }

  def getOrderingsField(mb: MethodBuilder): Stack = {
    import mb._
    thisVar.push().invokeV("orderings", classOf[Map[_, _]].asType)
  }

  def defOrderings(methodDef: MethodDef): Unit = {
    methodDef.newMethod("orderings", classOf[Map[_, _]].asType, Seq.empty,
      new MethodSignatureBuilder()
        .newReturnType {
          _.newClassType(classOf[Map[_, _]].asType) {
            _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[BranchKey].asType)
              .newTypeArgument(SignatureVisitor.INSTANCEOF) {
                _.newClassType(classOf[Ordering[_]].asType) {
                  _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[ShuffleKey].asType)
                }
              }
          }
        }
        .build()) { mb =>
        import mb._
        thisVar.push().getField("orderings", classOf[Map[_, _]].asType).unlessNotNull {
          thisVar.push().putField("orderings", classOf[Map[_, _]].asType, initOrderings(mb))
        }
        `return`(thisVar.push().getField("orderings", classOf[Map[_, _]].asType))
      }
  }

  private def initOrderings(mb: MethodBuilder): Stack = {
    import mb._
    val builder = getStatic(Map.getClass.asType, "MODULE$", Map.getClass.asType)
      .invokeV("newBuilder", classOf[mutable.Builder[_, _]].asType)
    for {
      output <- subplanOutputs.sortBy(_.getOperator.getSerialNumber)
      outputInfo <- Option(output.getAttribute(classOf[SubPlanOutputInfo]))
      partitionInfo <- Option(outputInfo.getPartitionInfo)
    } {
      builder.invokeI(
        NameTransformer.encode("+="),
        classOf[mutable.Builder[_, _]].asType,
        getStatic(Tuple2.getClass.asType, "MODULE$", Tuple2.getClass.asType).
          invokeV("apply", classOf[(_, _)].asType,
            branchKeys.getField(mb, output.getOperator).asType(classOf[AnyRef].asType), {
              val ordering = pushNew(classOf[ShuffleKey.SortOrdering].asType)
              ordering.dup().invokeInit(
                ldc(partitionInfo.getGrouping.size), {
                  val arr = pushNewArray(Type.BOOLEAN_TYPE, partitionInfo.getOrdering.size)
                  for {
                    (ordering, i) <- partitionInfo.getOrdering.zipWithIndex
                  } {
                    arr.dup().astore(
                      ldc(i),
                      ldc(ordering.getDirection == Group.Direction.ASCENDANT))
                  }
                  arr
                })
              ordering.asType(classOf[AnyRef].asType)
            })
          .asType(classOf[AnyRef].asType))
    }
    builder.invokeI("result", classOf[AnyRef].asType).cast(classOf[Map[_, _]].asType)
  }
}
