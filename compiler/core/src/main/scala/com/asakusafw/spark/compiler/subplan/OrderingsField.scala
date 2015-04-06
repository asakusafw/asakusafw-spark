package com.asakusafw.spark.compiler
package subplan

import scala.collection.mutable
import scala.collection.JavaConversions._
import scala.reflect.NameTransformer

import org.apache.spark.Partitioner
import org.objectweb.asm.Type
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.lang.compiler.api.JobflowProcessor.{ Context => JPContext }
import com.asakusafw.lang.compiler.model.graph.Group
import com.asakusafw.lang.compiler.planning.SubPlan
import com.asakusafw.lang.compiler.planning.spark.PartitioningParameters
import com.asakusafw.spark.runtime.driver.ShuffleKey
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._

trait OrderingsField extends ClassBuilder {

  def flowId: String

  def jpContext: JPContext

  def subplanOutputs: Seq[SubPlan.Output]

  def defOrderingsField(fieldDef: FieldDef): Unit = {
    fieldDef.newFinalField("orderings", classOf[Map[_, _]].asType,
      new TypeSignatureBuilder()
        .newClassType(classOf[Map[_, _]].asType) {
          _.newTypeArgument(SignatureVisitor.INSTANCEOF, Type.LONG_TYPE.boxed)
            .newTypeArgument(SignatureVisitor.INSTANCEOF) {
              _.newClassType(classOf[Ordering[_]].asType) {
                _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[ShuffleKey].asType)
              }
            }
        }
        .build())
  }

  def initOrderingsField(mb: MethodBuilder): Unit = {
    import mb._
    thisVar.push().putField("orderings", classOf[Map[_, _]].asType, initOrderings(mb))
  }

  def initOrderings(mb: MethodBuilder): Stack = {
    import mb._
    val builder = getStatic(Map.getClass.asType, "MODULE$", Map.getClass.asType)
      .invokeV("newBuilder", classOf[mutable.Builder[_, _]].asType)
    subplanOutputs.sortBy(_.getOperator.getOriginalSerialNumber).foreach { output =>
      val op = output.getOperator
      Option(output.getAttribute(classOf[PartitioningParameters])).foreach { params =>
        builder.invokeI(
          NameTransformer.encode("+="),
          classOf[mutable.Builder[_, _]].asType,
          getStatic(Tuple2.getClass.asType, "MODULE$", Tuple2.getClass.asType).
            invokeV("apply", classOf[(_, _)].asType,
              ldc(op.getOriginalSerialNumber).box().asType(classOf[AnyRef].asType), {
                val ordering = pushNew(classOf[ShuffleKey.SortOrdering].asType)
                ordering.dup().invokeInit({
                  val builder = getStatic(Seq.getClass.asType, "MODULE$", Seq.getClass.asType)
                    .invokeV("newBuilder", classOf[mutable.Builder[_, _]].asType)

                  params.getKey.getOrdering.foreach { ordering =>
                    builder.invokeI(
                      NameTransformer.encode("+="),
                      classOf[mutable.Builder[_, _]].asType,
                      ldc(ordering.getDirection == Group.Direction.ASCENDANT).box().asType(classOf[AnyRef].asType))
                  }

                  builder.invokeI("result", classOf[AnyRef].asType).cast(classOf[Seq[_]].asType)
                })
                ordering.asType(classOf[AnyRef].asType)
              })
            .asType(classOf[AnyRef].asType))
      }
    }
    builder.invokeI("result", classOf[AnyRef].asType).cast(classOf[Map[_, _]].asType)
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
            _.newTypeArgument(SignatureVisitor.INSTANCEOF, Type.LONG_TYPE.boxed)
              .newTypeArgument(SignatureVisitor.INSTANCEOF) {
                _.newClassType(classOf[Ordering[_]].asType) {
                  _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[ShuffleKey].asType)
                }
              }
          }
        }
        .build()) { mb =>
        import mb._
        `return`(thisVar.push().getField("orderings", classOf[Map[_, _]].asType))
      }
  }
}
