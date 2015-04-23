package com.asakusafw.spark.compiler

import java.lang.{ Boolean => JBoolean }
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.generic.Growable
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
import com.asakusafw.spark.compiler.planning.{
  SparkPlanning,
  SubPlanInfo,
  SubPlanInputInfo,
  SubPlanOutputInfo
}
import com.asakusafw.spark.compiler.serializer.{
  BranchKeySerializerClassBuilder,
  BroadcastIdSerializerClassBuilder,
  KryoRegistratorCompiler
}
import com.asakusafw.spark.compiler.spi.SubPlanCompiler
import com.asakusafw.spark.compiler.subplan.{ BranchKeysClassBuilder, BroadcastIdsClassBuilder }
import com.asakusafw.spark.runtime.driver.{ BroadcastId, ShuffleKey }
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
      val branchKeysClassBuilder = new BranchKeysClassBuilder(source.getFlowId)
      val broadcastIdsClassBuilder = new BroadcastIdsClassBuilder(source.getFlowId)
      implicit val context = SubPlanCompiler.Context(
        flowId = source.getFlowId,
        jpContext = jpContext,
        externalInputs = mutable.Map.empty,
        branchKeys = branchKeysClassBuilder,
        broadcastIds = broadcastIdsClassBuilder,
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
                val primaryOperator = subplan.getAttribute(classOf[SubPlanInfo]).getPrimaryOperator
                val compiler = subplanCompilers.find(_.support(primaryOperator)).get
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
                      val key = input.getAttribute(classOf[SubPlanInputInfo]).getPartitionInfo
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
                            context.broadcastIds.getField(mb, input.getOperator))
                          .asType(classOf[AnyRef].asType),
                        thisVar.push().invokeV(
                          "broadcastAsHash",
                          classOf[Broadcast[_]].asType,
                          scVar.push(),
                          {
                            val builder = getStatic(Seq.getClass.asType, "MODULE$", Seq.getClass.asType)
                              .invokeV("newBuilder", classOf[mutable.Builder[_, _]].asType)

                            input.getOpposites.toSeq.map(_.getOperator).foreach { marker =>
                              builder.invokeI(NameTransformer.encode("+="), classOf[mutable.Builder[_, _]].asType,
                                rddsVar.push().invokeI(
                                  "apply",
                                  classOf[AnyRef].asType,
                                  context.branchKeys.getField(mb, marker).asType(classOf[AnyRef].asType)))
                            }

                            builder.invokeI("result", classOf[AnyRef].asType).cast(classOf[Seq[_]].asType)
                          },
                          {
                            getStatic(Option.getClass.asType, "MODULE$", Option.getClass.asType)
                              .invokeV("apply", classOf[Option[_]].asType, {
                                val sort = pushNew(classOf[ShuffleKey.SortOrdering].asType)
                                sort.dup().invokeInit(
                                  ldc(key.getGrouping.size), {
                                    val arr = pushNewArray(Type.BOOLEAN_TYPE, key.getOrdering.size)
                                    key.getOrdering.zipWithIndex.foreach {
                                      case (ordering, i) =>
                                        arr.dup().astore(ldc(i),
                                          ldc(ordering.getDirection == Group.Direction.ASCENDANT))
                                    }
                                    arr
                                  })
                                sort
                              }.asType(classOf[AnyRef].asType))
                          },
                          {
                            val grouping = pushNew(classOf[ShuffleKey.GroupingOrdering].asType)
                            grouping.dup().invokeInit(ldc(key.getGrouping.size))
                            grouping
                          },
                          {
                            val partitioner = pushNew(classOf[HashPartitioner].asType)
                            partitioner.dup().invokeInit(ldc(1))
                            partitioner.asType(classOf[Partitioner].asType)
                          })
                          .asType(classOf[AnyRef].asType))
                        .asType(classOf[AnyRef].asType)
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

                rddsVar.push().invokeI(
                  NameTransformer.encode("++="),
                  classOf[Growable[_]].asType,
                  resultVar.push().asType(classOf[TraversableOnce[_]].asType))
                  .pop()

                `return`()
              }
          }

          val branchKeysType = jpContext.addClass(branchKeysClassBuilder)
          val broadcastIdsType = jpContext.addClass(broadcastIdsClassBuilder)

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
    SparkPlanning.plan(jpContext, source).getPlan
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
