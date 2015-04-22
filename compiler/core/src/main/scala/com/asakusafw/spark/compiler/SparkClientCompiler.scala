package com.asakusafw.spark.compiler

import java.lang.{ Boolean => JBoolean }
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable
import scala.collection.JavaConversions._
import scala.reflect.NameTransformer

import org.apache.hadoop.conf.Configuration
import org.apache.spark.{ HashPartitioner, Partitioner, SparkContext }
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.objectweb.asm.Type
import org.objectweb.asm.signature.SignatureVisitor
import org.slf4j.LoggerFactory

import com.asakusafw.lang.compiler.api.{ Exclusive, JobflowProcessor }
import com.asakusafw.lang.compiler.api.JobflowProcessor.{ Context => JPContext }
import com.asakusafw.lang.compiler.api.reference.CommandToken
import com.asakusafw.lang.compiler.analyzer.util.OperatorUtil
import com.asakusafw.lang.compiler.common.Location
import com.asakusafw.lang.compiler.inspection.driver.InspectionExtension
import com.asakusafw.lang.compiler.model.description.{ ClassDescription, TypeDescription }
import com.asakusafw.lang.compiler.model.graph._
import com.asakusafw.lang.compiler.planning._
import com.asakusafw.lang.compiler.planning.spark.{ DominantOperator, PartitioningParameters }
import com.asakusafw.spark.compiler.planning.SparkPlanning
import com.asakusafw.spark.compiler.serializer.{
  BranchKeySerializerClassBuilder,
  BroadcastIdSerializerClassBuilder,
  KryoRegistratorCompiler
}
import com.asakusafw.spark.compiler.spi.SubPlanCompiler
import com.asakusafw.spark.compiler.subplan.{ BranchKeysClassBuilder, BroadcastIdsClassBuilder }
import com.asakusafw.spark.runtime.driver.BroadcastId
import com.asakusafw.spark.runtime.rdd.BranchKey
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._
import com.asakusafw.utils.graph.Graphs

@Exclusive
class SparkClientCompiler extends JobflowProcessor {

  private val Logger = LoggerFactory.getLogger(getClass)

  import SparkClientCompiler._

  override def process(jpContext: JPContext, source: Jobflow): Unit = {

    if (Logger.isDebugEnabled) {
      Logger.debug("Start Asakusafw Spark compiler.")
    }

    val plan = preparePlan(jpContext, source)

    InspectionExtension.inspect(jpContext, Location.of("META-INF/asakusa-spark/plan.json", '/'), plan)

    if (JBoolean.parseBoolean(jpContext.getOptions.get(Options.SparkPlanVerify, false.toString)) == false) {

      val subplans = Graphs.sortPostOrder(Planning.toDependencyGraph(plan)).toSeq

      val subplanCompilers = SubPlanCompiler(jpContext.getClassLoader)
      implicit val context = SubPlanCompiler.Context(
        flowId = source.getFlowId,
        jpContext = jpContext,
        externalInputs = mutable.Map.empty,
        branchKeys = new BranchKeysClassBuilder(source.getFlowId),
        broadcastIds = new BroadcastIdsClassBuilder(source.getFlowId),
        shuffleKeyTypes = mutable.Set.empty)

      val builder = new SparkClientClassBuilder(source.getFlowId) {

        override def defFields(fieldDef: FieldDef): Unit = {
          fieldDef.newField("sc", classOf[SparkContext].asType)
          fieldDef.newField("hadoopConf", classOf[Broadcast[Configuration]].asType,
            new TypeSignatureBuilder()
              .newClassType(classOf[Broadcast[_]].asType) {
                _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[Configuration].asType)
              }
              .build())
          fieldDef.newField("rdds", classOf[mutable.Map[BranchKey, RDD[_]]].asType,
            new TypeSignatureBuilder()
              .newClassType(classOf[mutable.Map[_, _]].asType) {
                _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[BranchKey].asType)
                  .newTypeArgument(SignatureVisitor.INSTANCEOF) {
                    _.newClassType(classOf[RDD[_]].asType) {
                      _.newTypeArgument()
                    }
                  }
              }
              .build())
        }

        override def defMethods(methodDef: MethodDef): Unit = {
          methodDef.newMethod("execute", Type.INT_TYPE, Seq(classOf[SparkContext].asType, classOf[Broadcast[Configuration]].asType),
            new MethodSignatureBuilder()
              .newParameterType(classOf[SparkContext].asType)
              .newParameterType {
                _.newClassType(classOf[Broadcast[_]].asType) {
                  _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[Configuration].asType)
                }
              }
              .newReturnType(Type.INT_TYPE)
              .build()) { mb =>
              import mb._
              val scVar = `var`(classOf[SparkContext].asType, thisVar.nextLocal)
              val hadoopConfVar = `var`(classOf[Broadcast[Configuration]].asType, scVar.nextLocal)
              thisVar.push().putField("sc", classOf[SparkContext].asType, scVar.push())
              thisVar.push().putField("hadoopConf", classOf[Broadcast[Configuration]].asType, hadoopConfVar.push())
              thisVar.push().putField("rdds", classOf[mutable.Map[BranchKey, RDD[_]]].asType,
                getStatic(mutable.Map.getClass.asType, "MODULE$", mutable.Map.getClass.asType)
                  .invokeV("empty", classOf[mutable.Map[BranchKey, RDD[_]]].asType))

              subplans.zipWithIndex.foreach {
                case (_, i) =>
                  thisVar.push().invokeV(s"execute${i}")
              }

              `return`(ldc(0))
            }

          subplans.zipWithIndex.foreach {
            case (subplan, i) =>
              methodDef.newMethod(s"execute${i}", Seq.empty) { mb =>
                import mb._
                val dominant = subplan.getAttribute(classOf[DominantOperator]).getDominantOperator
                val compiler = subplanCompilers.find(_.support(dominant)).get
                val driverType = compiler.compile(subplan)

                val scVar = thisVar.push().getField("sc", classOf[SparkContext].asType).store(thisVar.nextLocal)
                val hadoopConfVar = thisVar.push().getField("hadoopConf", classOf[Broadcast[Configuration]].asType).store(scVar.nextLocal)
                val rddsVar = thisVar.push().getField("rdds", classOf[mutable.Map[BranchKey, RDD[_]]].asType).store(hadoopConfVar.nextLocal)
                val nextLocal = new AtomicInteger(rddsVar.nextLocal)

                val broadcastsVar = {
                  val builder = getStatic(Map.getClass.asType, "MODULE$", Map.getClass.asType)
                    .invokeV("newBuilder", classOf[mutable.Builder[_, _]].asType)
                  subplan.getInputs.toSet[SubPlan.Input]
                    .filter(_.getOperator.getAttribute(classOf[PlanMarker]) == PlanMarker.BROADCAST)
                    .foreach { input =>
                      val dataModelRef = context.jpContext.getDataModelLoader.load(input.getOperator.getInput.getDataType)
                      val key = input.getAttribute(classOf[PartitioningParameters]).getKey
                      val groupings = key.getGrouping.toSeq.map { grouping =>
                        (dataModelRef.findProperty(grouping).getType.asType, true)
                      }
                      val orderings = groupings ++ key.getOrdering.toSeq.map { ordering =>
                        (dataModelRef.findProperty(ordering.getPropertyName).getType.asType,
                          ordering.getDirection == Group.Direction.ASCENDANT)
                      }

                      builder.invokeI(
                        NameTransformer.encode("+="),
                        classOf[mutable.Builder[_, _]].asType,
                        getStatic(Tuple2.getClass.asType, "MODULE$", Tuple2.getClass.asType)
                          .invokeV(
                            "apply",
                            classOf[(BroadcastId, Broadcast[_])].asType,
                            getStatic(
                              context.broadcastIds.thisType,
                              context.broadcastIds.getField(input.getOperator.getOriginalSerialNumber),
                              classOf[BroadcastId].asType)
                              .asType(classOf[AnyRef].asType),
                            thisVar.push().invokeV(
                              "broadcastAsHash",
                              classOf[Broadcast[_]].asType,
                              scVar.push(),
                              {
                                val builder = getStatic(Seq.getClass.asType, "MODULE$", Seq.getClass.asType)
                                  .invokeV("newBuilder", classOf[mutable.Builder[_, _]].asType)

                                input.getOpposites.toSeq.map(_.getOperator.getSerialNumber).foreach { sn =>
                                  builder.invokeI(NameTransformer.encode("+="), classOf[mutable.Builder[_, _]].asType,
                                    rddsVar.push().invokeI(
                                      "apply",
                                      classOf[AnyRef].asType,
                                      getStatic(
                                        context.branchKeys.thisType,
                                        context.branchKeys.getField(sn),
                                        classOf[BranchKey].asType).asType(classOf[AnyRef].asType)))
                                }

                                builder.invokeI("result", classOf[AnyRef].asType).cast(classOf[Seq[_]].asType)
                              },
                              {
                                val builder = getStatic(Seq.getClass.asType, "MODULE$", Seq.getClass.asType)
                                  .invokeV("newBuilder", classOf[mutable.Builder[_, _]].asType)

                                key.getOrdering.foreach { ordering =>
                                  builder.invokeI(
                                    NameTransformer.encode("+="),
                                    classOf[mutable.Builder[_, _]].asType,
                                    ldc(ordering.getDirection == Group.Direction.ASCENDANT).box().asType(classOf[AnyRef].asType))
                                }

                                builder.invokeI("result", classOf[AnyRef].asType).cast(classOf[Seq[_]].asType)
                              },
                              {
                                val partitioner = pushNew(classOf[HashPartitioner].asType)
                                partitioner.dup().invokeInit(ldc(1))
                                partitioner.asType(classOf[Partitioner].asType)
                              })
                              .asType(classOf[AnyRef].asType))
                          .asType(classOf[AnyRef].asType))
                    }
                  val broadcasts = builder.invokeI("result", classOf[AnyRef].asType).cast(classOf[Map[_, _]].asType)
                  broadcasts.store(nextLocal.getAndAdd(broadcasts.size))
                }

                val instantiator = compiler.instantiator
                val driverVar = instantiator.newInstance(driverType, subplan)(
                  instantiator.Context(mb, scVar, hadoopConfVar, broadcastsVar, rddsVar,
                    nextLocal, source.getFlowId, jpContext, context.branchKeys))
                val rdds = driverVar.push().invokeV("execute", classOf[Map[BranchKey, RDD[_]]].asType)
                val resultVar = rdds.store(nextLocal.getAndAdd(rdds.size))

                subplan.getOutputs.collect {
                  case output if output.getOpposites.size > 0 => output.getOperator
                }.foreach { marker =>
                  rddsVar.push().invokeI(
                    NameTransformer.encode("+="),
                    classOf[mutable.MapLike[_, _, _]].asType,
                    getStatic(Tuple2.getClass.asType, "MODULE$", Tuple2.getClass.asType)
                      .invokeV(
                        "apply",
                        classOf[(BranchKey, RDD[_])].asType,
                        getStatic(
                          context.branchKeys.thisType,
                          context.branchKeys.getField(marker.getSerialNumber),
                          classOf[BranchKey].asType)
                          .asType(classOf[AnyRef].asType),
                        resultVar.push().invokeI(
                          "apply",
                          classOf[AnyRef].asType,
                          getStatic(
                            context.branchKeys.thisType,
                            context.branchKeys.getField(marker.getOriginalSerialNumber),
                            classOf[BranchKey].asType).asType(classOf[AnyRef].asType))
                          .cast(classOf[RDD[_]].asType)
                          .asType(classOf[AnyRef].asType)))
                    .pop()
                }
                `return`()
              }
          }

          val branchKeysType = jpContext.addClass(context.branchKeys)
          val broadcastIdsType = jpContext.addClass(context.broadcastIds)

          val registrator = KryoRegistratorCompiler.compile(
            OperatorUtil.collectDataTypes(
              plan.getElements.toSet[SubPlan].flatMap(_.getOperators.toSet[Operator]))
              .toSet[TypeDescription]
              .map(_.asType) ++
              context.shuffleKeyTypes,
            jpContext.addClass(new BranchKeySerializerClassBuilder(context.flowId, branchKeysType)),
            jpContext.addClass(new BroadcastIdSerializerClassBuilder(context.flowId, broadcastIdsType)))(
              KryoRegistratorCompiler.Context(source.getFlowId, jpContext))

          methodDef.newMethod("kryoRegistrator", classOf[String].asType, Seq.empty) { mb =>
            import mb._
            `return`(ldc(registrator.getClassName))
          }
        }
      }

      val client = jpContext.addClass(builder)

      jpContext.addTask(ModuleName, ProfileName, Command,
        Seq(CommandToken.BATCH_ID, CommandToken.FLOW_ID, CommandToken.EXECUTION_ID, CommandToken.BATCH_ARGUMENTS,
          CommandToken.of(client.getClassName)))
    }
  }

  def preparePlan(jpContext: JPContext, source: Jobflow): Plan = {
    SparkPlanning.plan(jpContext, source, source.getOperatorGraph.copy).getPlan
  }
}

object SparkClientCompiler {

  val ModuleName: String = "spark"

  val ProfileName: String = "spark"

  val Command: Location = Location.of("spark/bin/spark-execute.sh")

  object Options {
    val SparkPlanVerify = "spark.plan.verify"
  }
}
