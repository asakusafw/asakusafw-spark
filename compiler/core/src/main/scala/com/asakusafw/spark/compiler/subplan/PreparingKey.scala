package com.asakusafw.spark.compiler
package subplan

import scala.collection.mutable
import scala.collection.JavaConversions._
import scala.reflect.NameTransformer

import org.objectweb.asm.{ Opcodes, Type }
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.lang.compiler.api.JobflowProcessor.{ Context => JPContext }
import com.asakusafw.lang.compiler.model.graph.MarkerOperator
import com.asakusafw.lang.compiler.planning.SubPlan
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.spark.compiler.planning.SubPlanOutputInfo
import com.asakusafw.spark.compiler.spi.AggregationCompiler
import com.asakusafw.spark.runtime.aggregation.Aggregation
import com.asakusafw.spark.runtime.driver.ShuffleKey
import com.asakusafw.spark.runtime.rdd.BranchKey
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._

trait PreparingKey extends ClassBuilder {
  this: AggregationsField =>

  def flowId: String

  def jpContext: JPContext

  def branchKeys: BranchKeysClassBuilder

  def subplanOutputs: Seq[SubPlan.Output]

  lazy val shuffleKeyTypes: Map[Long, (Type, Boolean)] = {
    (for {
      output <- subplanOutputs
      outputInfo <- Option(output.getAttribute(classOf[SubPlanOutputInfo]))
      partitionInfo <- Option(outputInfo.getPartitionInfo)
    } yield {
      val op = output.getOperator
      val dataModelRef = jpContext.getDataModelLoader.load(op.getInput.getDataType)
      val dataModelType = dataModelRef.getDeclaration.asType

      val shuffleKeyType = ShuffleKeyClassBuilder.getOrCompile(jpContext)(
        flowId,
        dataModelType,
        partitionInfo.getGrouping.map { grouping =>
          val property = dataModelRef.findProperty(grouping)
          (property.getDeclaration.getName, property.getType.asType)
        },
        partitionInfo.getOrdering.map { ordering =>
          val property = dataModelRef.findProperty(ordering.getPropertyName)
          (property.getDeclaration.getName, property.getType.asType)
        })

      val aggregation =
        Option(output.getAttribute(classOf[SubPlanOutputInfo]).getAggregationInfo).exists {
          aggregationOperator =>
            AggregationCompiler.support(aggregationOperator)(AggregationCompiler.Context(flowId, jpContext))
        }
      op.getOriginalSerialNumber -> (shuffleKeyType, aggregation)
    }).toMap
  }

  def defShuffleKeyFields(fieldDef: FieldDef): Unit = {
    fieldDef.newField(
      Opcodes.ACC_PRIVATE | Opcodes.ACC_TRANSIENT,
      "shuffleKeys",
      classOf[mutable.Map[_, _]].asType,
      new TypeSignatureBuilder()
        .newClassType(classOf[mutable.Map[_, _]].asType) {
          _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[BranchKey].asType)
            .newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[ShuffleKey].asType)
        }
        .build())
  }

  def defShuffleKey(methodDef: MethodDef): Unit = {

    methodDef.newMethod("shuffleKey", classOf[ShuffleKey].asType,
      Seq(classOf[BranchKey].asType, classOf[AnyRef].asType)) { mb =>
        import mb._
        val branchVar = `var`(classOf[BranchKey].asType, thisVar.nextLocal)
        val valueVar = `var`(classOf[AnyRef].asType, branchVar.nextLocal)
        `return`(thisVar.push()
          .invokeV("shuffleKey", classOf[ShuffleKey].asType,
            branchVar.push(),
            valueVar.push().cast(classOf[DataModel[_]].asType)))
      }

    methodDef.newMethod("shuffleKey", classOf[ShuffleKey].asType,
      Seq(classOf[BranchKey].asType, classOf[DataModel[_]].asType)) { mb =>
        import mb._
        val branchVar = `var`(classOf[BranchKey].asType, thisVar.nextLocal)
        val valueVar = `var`(classOf[DataModel[_]].asType, branchVar.nextLocal)

        subplanOutputs.collect {
          case output if shuffleKeyTypes.contains(output.getOperator.getOriginalSerialNumber) =>
            val op = output.getOperator
            (op, shuffleKeyTypes(op.getOriginalSerialNumber))
        }.sortBy(_._1.getSerialNumber).foreach {
          case (op, (shuffleKeyType, aggregation)) =>

            val dataModelRef = jpContext.getDataModelLoader.load(op.getInput.getDataType)
            val dataModelType = dataModelRef.getDeclaration.asType

            branchVar.push().unlessNotEqual(
              getStatic(
                branchKeys.thisType,
                branchKeys.getField(op.getSerialNumber),
                classOf[BranchKey].asType)) {

                if (aggregation) {
                  val aggregationVar = getAggregationsField(mb)
                    .invokeI(
                      "get",
                      classOf[Option[_]].asType,
                      getStatic(
                        branchKeys.thisType,
                        branchKeys.getField(op.getSerialNumber),
                        classOf[BranchKey].asType)
                        .asType(classOf[AnyRef].asType))
                    .invokeV("orNull", classOf[AnyRef].asType,
                      getStatic(Predef.getClass.asType, "MODULE$", Predef.getClass.asType)
                        .invokeV("conforms", classOf[<:<[_, _]].asType))
                    .cast(classOf[Aggregation[_, _, _]].asType)
                    .store(valueVar.nextLocal)
                  aggregationVar.push().unlessNull {
                    aggregationVar.push().invokeV("mapSideCombine", Type.BOOLEAN_TYPE).unlessFalse {
                      val shuffleKey = pushNew(shuffleKeyType)
                      shuffleKey.dup().invokeInit(valueVar.push().cast(dataModelType))
                      `return`(shuffleKey)
                    }
                  }
                }
                thisVar.push().getField("shuffleKeys", classOf[mutable.Map[_, _]].asType).unlessNotNull {
                  thisVar.push().putField("shuffleKeys", classOf[mutable.Map[_, _]].asType,
                    getStatic(mutable.Map.getClass.asType, "MODULE$", mutable.Map.getClass.asType)
                      .invokeV("empty", classOf[mutable.Map[_, _]].asType))
                }
                val shuffleKeyVar = thisVar.push().getField("shuffleKeys", classOf[mutable.Map[_, _]].asType)
                  .invokeI(
                    "get",
                    classOf[Option[_]].asType,
                    getStatic(
                      branchKeys.thisType,
                      branchKeys.getField(op.getSerialNumber),
                      classOf[BranchKey].asType)
                      .asType(classOf[AnyRef].asType))
                  .invokeV("orNull", classOf[AnyRef].asType,
                    getStatic(Predef.getClass.asType, "MODULE$", Predef.getClass.asType)
                      .invokeV("conforms", classOf[<:<[_, _]].asType))
                  .cast(shuffleKeyType)
                  .store(valueVar.nextLocal)
                shuffleKeyVar.push().unlessNotNull {
                  pushNew0(shuffleKeyType).store(shuffleKeyVar.local)
                  thisVar.push().getField("shuffleKeys", classOf[mutable.Map[_, _]].asType)
                    .invokeI(
                      NameTransformer.encode("+="),
                      classOf[mutable.MapLike[_, _, _]].asType,
                      getStatic(Tuple2.getClass.asType, "MODULE$", Tuple2.getClass.asType)
                        .invokeV("apply", classOf[(_, _)].asType,
                          getStatic(
                            branchKeys.thisType,
                            branchKeys.getField(op.getSerialNumber),
                            classOf[BranchKey].asType)
                            .asType(classOf[AnyRef].asType),
                          shuffleKeyVar.push().asType(classOf[AnyRef].asType)))
                    .pop()
                }
                shuffleKeyVar.push().invokeV("copyFrom", valueVar.push().cast(dataModelType))
                `return`(shuffleKeyVar.push())
              }
        }
        `return`(pushNull(classOf[ShuffleKey].asType))
      }
  }
}
