package com.asakusafw.spark.compiler
package subplan

import scala.collection.mutable
import scala.collection.JavaConversions._
import scala.reflect.NameTransformer

import org.apache.spark.{ HashPartitioner, Partitioner, SparkContext }
import org.objectweb.asm.{ Opcodes, Type }
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.lang.compiler.api.JobflowProcessor.{ Context => JPContext }
import com.asakusafw.lang.compiler.planning.{ PlanMarker, SubPlan }
import com.asakusafw.spark.compiler.planning.SubPlanOutputInfo
import com.asakusafw.spark.runtime.rdd.BranchKey
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._

trait PartitionersField extends ClassBuilder {

  def flowId: String

  def jpContext: JPContext

  def branchKeys: BranchKeysClassBuilder

  def subplanOutputs: Seq[SubPlan.Output]

  def defPartitionersField(fieldDef: FieldDef): Unit = {
    fieldDef.newField(
      Opcodes.ACC_PRIVATE | Opcodes.ACC_TRANSIENT,
      "partitioners",
      classOf[Map[_, _]].asType,
      new TypeSignatureBuilder()
        .newClassType(classOf[Map[_, _]].asType) {
          _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[BranchKey].asType)
            .newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[Partitioner].asType)
        }
        .build())
  }

  def getPartitionersField(mb: MethodBuilder): Stack = {
    import mb._
    thisVar.push().invokeV("partitioners", classOf[Map[_, _]].asType)
  }

  def defPartitioners(methodDef: MethodDef): Unit = {
    methodDef.newMethod("partitioners", classOf[Map[_, _]].asType, Seq.empty,
      new MethodSignatureBuilder()
        .newReturnType {
          _.newClassType(classOf[Map[_, _]].asType) {
            _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[BranchKey].asType)
              .newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[Partitioner].asType)
          }
        }
        build ()) { mb =>
        import mb._
        thisVar.push().getField("partitioners", classOf[Map[_, _]].asType).unlessNotNull {
          thisVar.push().putField("partitioners", classOf[Map[_, _]].asType, initPartitioners(mb))
        }
        `return`(thisVar.push().getField("partitioners", classOf[Map[_, _]].asType))
      }
  }

  private def initPartitioners(mb: MethodBuilder): Stack = {
    import mb._
    val builder = getStatic(Map.getClass.asType, "MODULE$", Map.getClass.asType)
      .invokeV("newBuilder", classOf[mutable.Builder[_, _]].asType)
    for {
      output <- subplanOutputs.sortBy(_.getOperator.getSerialNumber)
      outputInfo <- Option(output.getAttribute(classOf[SubPlanOutputInfo]))
    } {
      builder.invokeI(
        NameTransformer.encode("+="),
        classOf[mutable.Builder[_, _]].asType,
        getStatic(Tuple2.getClass.asType, "MODULE$", Tuple2.getClass.asType).
          invokeV("apply", classOf[(_, _)].asType,
            getStatic(
              branchKeys.thisType,
              branchKeys.getField(output.getOperator.getSerialNumber),
              classOf[BranchKey].asType).asType(classOf[AnyRef].asType), {
              val partitioner = pushNew(classOf[HashPartitioner].asType)
              partitioner.dup().invokeInit(
                if (outputInfo.getOutputType == SubPlanOutputInfo.OutputType.BROADCAST) {
                  ldc(1)
                } else {
                  thisVar.push().invokeV("sc", classOf[SparkContext].asType)
                    .invokeV("defaultParallelism", Type.INT_TYPE)
                })
              partitioner.asType(classOf[AnyRef].asType)
            })
          .asType(classOf[AnyRef].asType))
    }
    builder.invokeI("result", classOf[AnyRef].asType).cast(classOf[Map[_, _]].asType)
  }
}
