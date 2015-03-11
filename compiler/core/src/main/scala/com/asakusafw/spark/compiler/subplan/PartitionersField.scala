package com.asakusafw.spark.compiler
package subplan

import scala.collection.mutable
import scala.collection.JavaConversions._
import scala.reflect.NameTransformer

import org.apache.spark.{ Partitioner, SparkContext }
import org.objectweb.asm.Type
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.lang.compiler.api.JobflowProcessor.{ Context => JPContext }
import com.asakusafw.lang.compiler.planning.SubPlan
import com.asakusafw.lang.compiler.planning.spark.PartitioningParameters
import com.asakusafw.spark.compiler.partitioner.GroupingPartitionerClassBuilder
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._

trait PartitionersField extends ClassBuilder {

  def flowId: String

  def jpContext: JPContext

  def subplanOutputs: Seq[SubPlan.Output]

  def defPartitionersField(fieldDef: FieldDef): Unit = {
    fieldDef.newFinalField("partitioners", classOf[Map[_, _]].asType,
      new TypeSignatureBuilder()
        .newClassType(classOf[Map[_, _]].asType) {
          _.newTypeArgument(SignatureVisitor.INSTANCEOF, Type.LONG_TYPE.boxed)
            .newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[Partitioner].asType)
        }
        .build())
  }

  def initPartitionersField(mb: MethodBuilder): Unit = {
    import mb._
    thisVar.push().putField("partitioners", classOf[Map[_, _]].asType, initPartitioners(mb))
  }

  def initPartitioners(mb: MethodBuilder): Stack = {
    import mb._
    val builder = getStatic(Map.getClass.asType, "MODULE$", Map.getClass.asType)
      .invokeV("newBuilder", classOf[mutable.Builder[_, _]].asType)
    subplanOutputs.sortBy(_.getOperator.getOriginalSerialNumber).foreach { output =>
      val op = output.getOperator
      Option(output.getAttribute(classOf[PartitioningParameters])).foreach { params =>
        val dataModelRef = jpContext.getDataModelLoader.load(op.getInput.getDataType)
        val group = params.getKey
        val properties: Seq[Type] =
          group.getGrouping.map { grouping =>
            dataModelRef.findProperty(grouping).getType.asType
          }
        val partitionerType = GroupingPartitionerClassBuilder.getOrCompile(flowId, properties, jpContext)

        builder.invokeI(
          NameTransformer.encode("+="),
          classOf[mutable.Builder[_, _]].asType,
          getStatic(Tuple2.getClass.asType, "MODULE$", Tuple2.getClass.asType).
            invokeV("apply", classOf[(_, _)].asType,
              ldc(op.getOriginalSerialNumber).box().asType(classOf[AnyRef].asType), {
                val partitioner = pushNew(partitionerType)
                partitioner.dup().invokeInit(
                  thisVar.push().invokeV("sc", classOf[SparkContext].asType)
                    .invokeV("defaultParallelism", Type.INT_TYPE))
                partitioner.asType(classOf[AnyRef].asType)
              })
            .asType(classOf[AnyRef].asType))
      }
    }
    builder.invokeI("result", classOf[AnyRef].asType).cast(classOf[Map[_, _]].asType)
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
            _.newTypeArgument(SignatureVisitor.INSTANCEOF, Type.LONG_TYPE.boxed)
              .newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[Partitioner].asType)
          }
        }
        build ()) { mb =>
        import mb._
        `return`(thisVar.push().getField("partitioners", classOf[Map[_, _]].asType))
      }
  }
}
