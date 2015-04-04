package com.asakusafw.spark.compiler

import java.io.{ File, FileOutputStream, PrintStream }
import java.lang.{ Boolean => JBoolean }
import java.nio.file.Files
import java.util.{ List => JList }
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable
import scala.collection.JavaConversions._
import scala.reflect.NameTransformer

import org.apache.hadoop.conf.Configuration
import org.apache.spark.{ HashPartitioner, Partitioner, SparkContext }
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.objectweb.asm.Type
import org.slf4j.LoggerFactory

import com.asakusafw.lang.compiler.api.JobflowProcessor
import com.asakusafw.lang.compiler.api.JobflowProcessor.{ Context => JPContext }
import com.asakusafw.lang.compiler.api.reference.CommandToken
import com.asakusafw.lang.compiler.analyzer.util.OperatorUtil
import com.asakusafw.lang.compiler.common.Location
import com.asakusafw.lang.compiler.model.description.{ ClassDescription, TypeDescription }
import com.asakusafw.lang.compiler.model.graph._
import com.asakusafw.lang.compiler.planning._
import com.asakusafw.lang.compiler.planning.spark.{ DominantOperator, LogicalSparkPlanner, PartitioningParameters }
import com.asakusafw.lang.compiler.planning.util.DotGenerator
import com.asakusafw.spark.compiler.serializer.KryoRegistratorCompiler
import com.asakusafw.spark.compiler.spi.SubPlanCompiler
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._
import com.asakusafw.utils.graph.Graphs

import resource._

class SparkClientCompiler extends JobflowProcessor {

  private val Logger = LoggerFactory.getLogger(getClass)

  import SparkClientCompiler._

  override def process(jpContext: JPContext, source: Jobflow): Unit = {

    if (Logger.isDebugEnabled) {
      Logger.debug("Start Asakusafw Spark compiler.")
    }

    val plan = preparePlan(
      source.getOperatorGraph.copy,
      source.getFlowId,
      JBoolean.parseBoolean(jpContext.getOptions.get(Options.SparkPlanDump, false.toString)))

    for {
      dotOutputStream <- managed(new PrintStream(
        jpContext.addResourceFile(Location.of("META-INF/asakusa-spark/plan.dot", '/'))))
    } {
      val dotGenerator = new DotGenerator
      val dot = dotGenerator.generate(plan, source.getFlowId)
      dotGenerator.save(dotOutputStream, dot)
    }

    if (JBoolean.parseBoolean(jpContext.getOptions.get(Options.SparkPlanVerify, false.toString)) == false) {

      val subplans = Graphs.sortPostOrder(Planning.toDependencyGraph(plan)).toSeq

      val subplanCompilers = SubPlanCompiler(jpContext.getClassLoader)
      implicit val context = SubPlanCompiler.Context(source.getFlowId, jpContext, mutable.Set.empty)

      val builder = new SparkClientClassBuilder(source.getFlowId) {

        override def defFields(fieldDef: FieldDef): Unit = {
          fieldDef.newField("sc", classOf[SparkContext].asType)
          fieldDef.newField("hadoopConf", classOf[Broadcast[Configuration]].asType)
          fieldDef.newField("rdds", classOf[mutable.Map[Long, RDD[_]]].asType)
        }

        override def defMethods(methodDef: MethodDef): Unit = {
          methodDef.newMethod("execute", Type.INT_TYPE, Seq(classOf[SparkContext].asType, classOf[Broadcast[Configuration]].asType)) { mb =>
            import mb._
            val scVar = `var`(classOf[SparkContext].asType, thisVar.nextLocal)
            val hadoopConfVar = `var`(classOf[Broadcast[Configuration]].asType, scVar.nextLocal)
            thisVar.push().putField("sc", classOf[SparkContext].asType, scVar.push())
            thisVar.push().putField("hadoopConf", classOf[Broadcast[Configuration]].asType, hadoopConfVar.push())
            thisVar.push().putField("rdds", classOf[mutable.Map[Long, RDD[_]]].asType,
              getStatic(mutable.Map.getClass.asType, "MODULE$", mutable.Map.getClass.asType)
                .invokeV("empty", classOf[mutable.Map[Long, RDD[_]]].asType))

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
                val rddsVar = thisVar.push().getField("rdds", classOf[mutable.Map[Long, RDD[_]]].asType).store(hadoopConfVar.nextLocal)
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
                            classOf[(Long, Broadcast[_])].asType,
                            ldc(input.getOperator.getOriginalSerialNumber).box().asType(classOf[AnyRef].asType),
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
                                      ldc(sn).box().asType(classOf[AnyRef].asType)))
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
                                partitioner.dup().invokeInit(scVar.push().invokeV("defaultParallelism", Type.INT_TYPE))
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
                    nextLocal, source.getFlowId, jpContext))
                val rdds = driverVar.push().invokeV("execute", classOf[Map[Long, RDD[_]]].asType)
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
                        classOf[(Long, RDD[_])].asType,
                        ldc(marker.getSerialNumber).box().asType(classOf[AnyRef].asType),
                        resultVar.push().invokeI(
                          "apply",
                          classOf[AnyRef].asType,
                          ldc(marker.getOriginalSerialNumber).box().asType(classOf[AnyRef].asType))
                          .cast(classOf[RDD[_]].asType)
                          .asType(classOf[AnyRef].asType)))
                    .pop()
                }
                `return`()
              }
          }

          val registrator = KryoRegistratorCompiler.compile(
            OperatorUtil.collectDataTypes(
              plan.getElements.toSet[SubPlan].flatMap(_.getOperators.toSet[Operator]))
              .toSet[TypeDescription]
              .map(_.asType),
            context.shuffleKeyTypes.toSet)(
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

  def preparePlan(graph: OperatorGraph, flowId: String, dump: Boolean): Plan = {
    if (dump) {
      val dir = Files.createTempDirectory("spark.plan.dump-").toFile.getAbsolutePath
      if (Logger.isDebugEnabled) {
        Logger.debug(s"spark.plan.dump: dot files to ${dir}")
      }
      var plan: Plan = null
      for {
        given <- managed(new FileOutputStream(new File(dir, s"${flowId}-0_given.dot")))
        normalized <- managed(new FileOutputStream(new File(dir, s"${flowId}-1_normalized.dot")))
        optimized <- managed(new FileOutputStream(new File(dir, s"${flowId}-2_optimized.dot")))
        marked <- managed(new FileOutputStream(new File(dir, s"${flowId}-3_marked.dot")))
        primitive <- managed(new FileOutputStream(new File(dir, s"${flowId}-4_primitive.dot")))
        unified <- managed(new FileOutputStream(new File(dir, s"${flowId}-5_unified.dot")))
      } {
        plan = new LogicalSparkPlanner().createPlanWithDumpStepByStep(
          graph,
          given, normalized, optimized, marked, primitive, unified).getPlan
      }
      plan
    } else {
      new LogicalSparkPlanner().createPlan(graph).getPlan
    }
  }
}

object SparkClientCompiler {

  val ModuleName: String = "spark"

  val ProfileName: String = "spark"

  val Command: Location = Location.of("spark/bin/spark-execute.sh")

  object Options {
    val SparkPlanVerify = "spark.plan.verify"
    val SparkPlanDump = "spark.plan.dump"
  }
}
