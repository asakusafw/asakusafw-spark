package com.asakusafw.spark.compiler

import java.io.PrintStream
import java.lang.{ Boolean => JBoolean }
import java.util.{ List => JList }
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable
import scala.collection.JavaConversions._

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
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
import com.asakusafw.lang.compiler.planning.spark.{ DominantOperator, LogicalSparkPlanner }
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

    val plan = preparePlan(source.getOperatorGraph.copy, source.getFlowId, jpContext)

    if (JBoolean.parseBoolean(jpContext.getOptions.get(SparkPlanVerifyOption, false.toString)) == false) {

      val registrator = KryoRegistratorCompiler.compile(
        OperatorUtil.collectDataTypes(plan.getElements.toSet[SubPlan].flatMap(_.getOperators.toSet[Operator]))
          .toSet[TypeDescription].map(_.asType))(
          KryoRegistratorCompiler.Context(source.getFlowId, jpContext))

      val subplans = Graphs.sortPostOrder(Planning.toDependencyGraph(plan)).toSeq

      val subplanCompilers = SubPlanCompiler(jpContext.getClassLoader)
      implicit val context = SubPlanCompiler.Context(source.getFlowId, jpContext)

      val builder = new SparkClientClassBuilder(source.getFlowId) {

        override def defMethods(methodDef: MethodDef): Unit = {
          methodDef.newMethod("execute", Type.INT_TYPE, Seq(classOf[SparkContext].asType, classOf[Broadcast[Configuration]].asType)) { mb =>
            import mb._
            val scVar = `var`(classOf[SparkContext].asType, thisVar.nextLocal)
            val hadoopConfVar = `var`(classOf[Broadcast[Configuration]].asType, scVar.nextLocal)
            val nextLocal = new AtomicInteger(hadoopConfVar.nextLocal)
            val rddVars = mutable.Map.empty[Long, Var] // MarkerOperator.serialNumber

            subplans.foreach { subplan =>
              val dominant = subplan.getAttribute(classOf[DominantOperator]).getDominantOperator
              val compiler = subplanCompilers.find(_.support(dominant)).get
              val instantiator = compiler.instantiator

              val driverType = compiler.compile(subplan)
              val driverVar = instantiator.newInstance(driverType, subplan)(
                instantiator.Context(mb, scVar, hadoopConfVar, rddVars, nextLocal, source.getFlowId, jpContext))
              val rdds = driverVar.push().invokeV("execute", classOf[Map[Long, RDD[_]]].asType)
              val rddsVar = rdds.store(nextLocal.getAndAdd(rdds.size))

              subplan.getOutputs.collect {
                case output if output.getOpposites.size > 0 => output.getOperator
              }.foreach { marker =>
                rddVars += (marker.getSerialNumber -> {
                  val rdd = rddsVar.push()
                    .invokeI("apply", classOf[AnyRef].asType,
                      ldc(marker.getOriginalSerialNumber).box().asType(classOf[AnyRef].asType))
                    .cast(classOf[RDD[_]].asType)
                  rdd.store(nextLocal.getAndAdd(rdd.size))
                })
              }
            }
            `return`(ldc(0))
          }

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

  def preparePlan(graph: OperatorGraph, flowId: String, jpContext: JPContext): Plan = {
    if (JBoolean.parseBoolean(jpContext.getOptions.get(SparkPlanDumpOption, false.toString)) == false) {
      val plan = new LogicalSparkPlanner().createPlan(graph).getPlan

      for {
        dotOutputStream <- managed(new PrintStream(
          jpContext.addResourceFile(Location.of("META-INF/asakusa-spark/plan.dot", '/'))))
      } {
        val dotGenerator = new DotGenerator
        val dot = dotGenerator.generate(plan, flowId)
        dotGenerator.save(dotOutputStream, dot)
      }

      plan
    } else {
      var plan: Plan = null
      for {
        given <- managed(
          jpContext.addResourceFile(Location.of("META-INF/asakusa-spark/0_given.dot", '/')))
        normalized <- managed(
          jpContext.addResourceFile(Location.of("META-INF/asakusa-spark/1_normalized.dot", '/')))
        optimized <- managed(
          jpContext.addResourceFile(Location.of("META-INF/asakusa-spark/2_optimized.dot", '/')))
        marked <- managed(
          jpContext.addResourceFile(Location.of("META-INF/asakusa-spark/3_marked.dot", '/')))
        primitive <- managed(
          jpContext.addResourceFile(Location.of("META-INF/asakusa-spark/4_primitive.dot", '/')))
        unified <- managed(
          jpContext.addResourceFile(Location.of("META-INF/asakusa-spark/5_unified.dot", '/')))
      } {
        plan = new LogicalSparkPlanner().createPlanWithDumpStepByStep(
          graph,
          given, normalized, optimized, marked, primitive, unified).getPlan
      }
      plan
    }
  }
}

object SparkClientCompiler {

  val ModuleName: String = "spark"

  val ProfileName: String = "spark"

  val Command: Location = Location.of("spark/bin/spark-execute.sh")

  val SparkPlanVerifyOption = "spark.plan.verify"

  val SparkPlanDumpOption = "spark.plan.dump"
}
