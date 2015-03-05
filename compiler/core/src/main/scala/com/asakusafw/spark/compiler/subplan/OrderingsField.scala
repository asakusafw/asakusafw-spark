package com.asakusafw.spark.compiler
package subplan

import scala.collection.mutable
import scala.collection.JavaConversions._
import scala.reflect.NameTransformer

import org.apache.spark.Partitioner
import org.objectweb.asm.Type
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.lang.compiler.api.JobflowProcessor.{ Context => JPContext }
import com.asakusafw.lang.compiler.model.graph.{ Group, MarkerOperator }
import com.asakusafw.lang.compiler.planning.spark.PartinioningParameters
import com.asakusafw.spark.compiler.ordering.OrderingClassBuilder
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._

trait OrderingsField extends ClassBuilder {

  def flowId: String

  def jpContext: JPContext

  def outputMarkers: Seq[MarkerOperator]

  def defOrderingsField(fieldDef: FieldDef): Unit = {
    fieldDef.newFinalField("orderings", classOf[Map[_, _]].asType,
      new TypeSignatureBuilder()
        .newClassType(classOf[Map[_, _]].asType) {
          _.newTypeArgument(SignatureVisitor.INSTANCEOF, Type.LONG_TYPE.boxed)
            .newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[Ordering[_]].asType)
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
    outputMarkers.sortBy(_.getOriginalSerialNumber).foreach { op =>
      Option(op.getAttribute(classOf[PartinioningParameters])).foreach { params =>
        val dataModelRef = jpContext.getDataModelLoader.load(op.getInput.getDataType)
        val group = params.getKey
        val properties: Seq[(Type, Boolean)] =
          group.getGrouping.map { grouping =>
            (dataModelRef.findProperty(grouping).getType.asType, true)
          } ++
            group.getOrdering.map { ordering =>
              (dataModelRef.findProperty(ordering.getPropertyName).getType.asType,
                ordering.getDirection == Group.Direction.ASCENDANT)
            }
        val ordering = OrderingClassBuilder.getOrCompile(flowId, properties, jpContext)

        builder.invokeI(
          NameTransformer.encode("+="),
          classOf[mutable.Builder[_, _]].asType,
          getStatic(Tuple2.getClass.asType, "MODULE$", Tuple2.getClass.asType).
            invokeV("apply", classOf[(_, _)].asType,
              ldc(op.getOriginalSerialNumber).box().asType(classOf[AnyRef].asType),
              pushNew0(ordering).asType(classOf[AnyRef].asType))
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
        .newFormalTypeParameter("K", classOf[AnyRef].asType)
        .newReturnType {
          _.newClassType(classOf[Map[_, _]].asType) {
            _.newTypeArgument(SignatureVisitor.INSTANCEOF, Type.LONG_TYPE.boxed)
              .newTypeArgument(SignatureVisitor.INSTANCEOF) {
                _.newClassType(classOf[Ordering[_]].asType) {
                  _.newTypeArgument(SignatureVisitor.INSTANCEOF) {
                    _.newTypeVariable("K")
                  }
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
