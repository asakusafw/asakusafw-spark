package com.asakusafw.spark.compiler

import java.lang.{ Boolean => JBoolean }
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.generic.Growable
import scala.collection.mutable
import scala.collection.JavaConversions._
import scala.concurrent.{ Await, Awaitable, Future }
import scala.concurrent.duration.Duration
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
  BroadcastInfo,
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

      val subplans = Graphs.sortPostOrder(Planning.toDependencyGraph(plan)).toSeq.zipWithIndex

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
          fieldDef.newField("rdds", classOf[mutable.Map[BranchKey, Future[RDD[_]]]].asType,
            new TypeSignatureBuilder()
              .newClassType(classOf[mutable.Map[_, _]].asType) {
                _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[BranchKey].asType)
                  .newTypeArgument(SignatureVisitor.INSTANCEOF) {
                    _.newClassType(classOf[Future[_]].asType) {
                      _.newTypeArgument(SignatureVisitor.INSTANCEOF) {
                        _.newClassType(classOf[RDD[_]].asType) {
                          _.newTypeArgument()
                        }
                      }
                    }
                  }
              }
              .build())
          fieldDef.newField("broadcasts", classOf[mutable.Map[BroadcastId, Future[Broadcast[Map[ShuffleKey, Seq[_]]]]]].asType,
            new TypeSignatureBuilder()
              .newClassType(classOf[mutable.Map[_, _]].asType) {
                _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[BroadcastId].asType)
                  .newTypeArgument(SignatureVisitor.INSTANCEOF) {
                    _.newClassType(classOf[Future[_]].asType) {
                      _.newTypeArgument(SignatureVisitor.INSTANCEOF) {
                        _.newClassType(classOf[Broadcast[_]].asType) {
                          _.newTypeArgument(SignatureVisitor.INSTANCEOF) {
                            _.newClassType(classOf[Map[_, _]].asType) {
                              _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[ShuffleKey].asType)
                                .newTypeArgument(SignatureVisitor.INSTANCEOF) {
                                  _.newClassType(classOf[Seq[_]].asType) {
                                    _.newTypeArgument()
                                  }
                                }
                            }
                          }
                        }
                      }
                    }
                  }
              }
              .build())
          fieldDef.newField("terminators", classOf[mutable.Set[Future[Unit]]].asType,
            new TypeSignatureBuilder()
              .newClassType(classOf[mutable.Set[Future[Unit]]].asType) {
                _.newTypeArgument(SignatureVisitor.INSTANCEOF) {
                  _.newClassType(classOf[Future[Unit]].asType) {
                    _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[Unit].asType)
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
              thisVar.push().putField("rdds", classOf[mutable.Map[BranchKey, Future[RDD[_]]]].asType,
                getStatic(mutable.Map.getClass.asType, "MODULE$", mutable.Map.getClass.asType)
                  .invokeV("empty", classOf[mutable.Map[BranchKey, Future[RDD[_]]]].asType))
              thisVar.push().putField("broadcasts", classOf[mutable.Map[BroadcastId, Future[Broadcast[Map[ShuffleKey, Seq[_]]]]]].asType,
                getStatic(mutable.Map.getClass.asType, "MODULE$", mutable.Map.getClass.asType)
                  .invokeV("empty", classOf[mutable.Map[BroadcastId, Future[Broadcast[Map[ShuffleKey, Seq[_]]]]]].asType))
              thisVar.push().putField("terminators", classOf[mutable.Set[Future[Unit]]].asType,
                getStatic(mutable.Set.getClass.asType, "MODULE$", mutable.Set.getClass.asType)
                  .invokeV("empty", classOf[mutable.Set[Future[Unit]]].asType))

              subplans.foreach {
                case (_, i) =>
                  thisVar.push().invokeV(s"execute${i}")
              }

              val iterVar = thisVar.push().getField("terminators", classOf[mutable.Set[Future[Unit]]].asType)
                .invokeI("iterator", classOf[Iterator[Future[Unit]]].asType)
                .store(hadoopConfVar.nextLocal)
              whileLoop(iterVar.push().invokeI("hasNext", Type.BOOLEAN_TYPE)) { ctrl =>
                getStatic(Await.getClass.asType, "MODULE$", Await.getClass.asType)
                  .invokeV("result", classOf[AnyRef].asType,
                    iterVar.push()
                      .invokeI("next", classOf[AnyRef].asType)
                      .cast(classOf[Future[Unit]].asType)
                      .asType(classOf[Awaitable[_]].asType),
                    getStatic(Duration.getClass.asType, "MODULE$", Duration.getClass.asType)
                      .invokeV("Inf", classOf[Duration.Infinite].asType)
                      .asType(classOf[Duration].asType))
                  .pop()
              }

              `return`(ldc(0))
            }

          subplans.foreach {
            case (subplan, i) =>
              methodDef.newMethod(s"execute${i}", Seq.empty) { mb =>
                import mb._
                val primaryOperator = subplan.getAttribute(classOf[SubPlanInfo]).getPrimaryOperator
                val compiler = subplanCompilers.find(_.support(primaryOperator)).get
                val driverType = compiler.compile(subplan)

                val scVar = thisVar.push().getField("sc", classOf[SparkContext].asType).store(thisVar.nextLocal)
                val hadoopConfVar = thisVar.push().getField("hadoopConf", classOf[Broadcast[Configuration]].asType).store(scVar.nextLocal)
                val rddsVar = thisVar.push().getField("rdds", classOf[mutable.Map[BranchKey, Future[RDD[_]]]].asType).store(hadoopConfVar.nextLocal)
                val terminatorsVar = thisVar.push().getField("terminators", classOf[mutable.Set[Future[Unit]]].asType).store(rddsVar.nextLocal)
                val nextLocal = new AtomicInteger(terminatorsVar.nextLocal)

                val broadcastsVar = {
                  val builder = getStatic(Map.getClass.asType, "MODULE$", Map.getClass.asType)
                    .invokeV("newBuilder", classOf[mutable.Builder[_, _]].asType)

                  for {
                    subPlanInput <- subplan.getInputs
                    inputInfo <- Option(subPlanInput.getAttribute(classOf[SubPlanInputInfo]))
                    if inputInfo.getInputType == SubPlanInputInfo.InputType.BROADCAST
                  } {
                    val prevSubPlanOutputs = subPlanInput.getOpposites
                    assert(prevSubPlanOutputs.size == 1)
                    val prevSubPlanOperator = prevSubPlanOutputs.head.getOperator

                    builder.invokeI(
                      NameTransformer.encode("+="),
                      classOf[mutable.Builder[_, _]].asType,
                      getStatic(Tuple2.getClass.asType, "MODULE$", Tuple2.getClass.asType)
                        .invokeV(
                          "apply",
                          classOf[(BroadcastId, Future[Broadcast[_]])].asType,
                          context.broadcastIds.getField(mb, subPlanInput.getOperator)
                            .asType(classOf[AnyRef].asType),
                          thisVar.push().getField("broadcasts", classOf[mutable.Map[BroadcastId, Future[Broadcast[Map[ShuffleKey, Seq[_]]]]]].asType)
                            .invokeI(
                              "apply",
                              classOf[AnyRef].asType,
                              context.broadcastIds.getField(mb, prevSubPlanOperator)
                                .asType(classOf[AnyRef].asType))
                            .cast(classOf[Future[Broadcast[Map[ShuffleKey, Seq[_]]]]].asType)
                            .asType(classOf[AnyRef].asType))
                        .asType(classOf[AnyRef].asType))
                  }

                  val broadcasts = builder.invokeI("result", classOf[AnyRef].asType).cast(classOf[Map[_, _]].asType)
                  broadcasts.store(nextLocal.getAndAdd(broadcasts.size))
                }

                val instantiator = compiler.instantiator
                val driverVar = instantiator.newInstance(driverType, subplan)(
                  instantiator.Context(mb, scVar, hadoopConfVar, broadcastsVar, rddsVar, terminatorsVar,
                    nextLocal, source.getFlowId, jpContext, context.branchKeys))
                val rdds = driverVar.push().invokeV("execute", classOf[Map[BranchKey, Future[RDD[(ShuffleKey, _)]]]].asType)
                val resultVar = rdds.store(nextLocal.getAndAdd(rdds.size))

                for {
                  subPlanOutput <- subplan.getOutputs
                  outputInfo <- Option(subPlanOutput.getAttribute(classOf[SubPlanOutputInfo]))
                  if outputInfo.getOutputType == SubPlanOutputInfo.OutputType.BROADCAST
                  broadcastInfo <- Option(subPlanOutput.getAttribute(classOf[BroadcastInfo]))
                } {
                  val dataModelRef = context.jpContext.getDataModelLoader.load(subPlanOutput.getOperator.getInput.getDataType)
                  val key = broadcastInfo.getFormatInfo
                  val groupings = key.getGrouping.toSeq.map { grouping =>
                    (dataModelRef.findProperty(grouping).getType.asType, true)
                  }
                  val orderings = groupings ++ key.getOrdering.toSeq.map { ordering =>
                    (dataModelRef.findProperty(ordering.getPropertyName).getType.asType,
                      ordering.getDirection == Group.Direction.ASCENDANT)
                  }

                  thisVar.push().getField("broadcasts", classOf[mutable.Map[BroadcastId, Future[Broadcast[Map[ShuffleKey, Seq[_]]]]]].asType)
                    .invokeI(
                      NameTransformer.encode("+="),
                      classOf[Growable[_]].asType,
                      getStatic(Tuple2.getClass.asType, "MODULE$", Tuple2.getClass.asType)
                        .invokeV("apply", classOf[(_, _)].asType,
                          context.broadcastIds.getField(mb, subPlanOutput.getOperator)
                            .asType(classOf[AnyRef].asType),
                          thisVar.push().invokeV(
                            "broadcastAsHash",
                            classOf[Future[Broadcast[_]]].asType,
                            scVar.push(),
                            resultVar.push().invokeI(
                              "apply",
                              classOf[AnyRef].asType,
                              context.branchKeys.getField(mb, subPlanOutput.getOperator)
                                .asType(classOf[AnyRef].asType))
                              .cast(classOf[Future[RDD[(ShuffleKey, _)]]].asType),
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
                        .asType(classOf[AnyRef].asType))
                    .pop()
                }

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
