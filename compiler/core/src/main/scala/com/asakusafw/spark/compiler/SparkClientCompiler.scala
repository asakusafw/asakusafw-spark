package com.asakusafw.spark.compiler

import java.io.{ PrintWriter, StringWriter }
import java.util.{ List => JList }
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable
import scala.collection.JavaConversions._

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.objectweb.asm.Type
import org.slf4j.LoggerFactory

import com.asakusafw.lang.compiler.api.JobflowProcessor
import com.asakusafw.lang.compiler.api.JobflowProcessor.{ Context => JPContext }
import com.asakusafw.lang.compiler.api.reference.CommandToken
import com.asakusafw.lang.compiler.common.Location
import com.asakusafw.lang.compiler.model.description.ClassDescription
import com.asakusafw.lang.compiler.model.graph._
import com.asakusafw.lang.compiler.planning._
import com.asakusafw.lang.compiler.planning.spark.{ DominantOperator, LogicalSparkPlanner }
import com.asakusafw.lang.compiler.planning.util.DotGenerator
import com.asakusafw.spark.compiler.spi.SubPlanCompiler
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._
import com.asakusafw.utils.graph.Graphs

import resource._

class SparkClientCompiler extends JobflowProcessor {

  private val Logger = LoggerFactory.getLogger(getClass)

  import SparkClientCompiler._

  override def process(jpContext: JPContext, source: Jobflow): Unit = {
    val plan = preparePlan(source.getOperatorGraph.copy)
    val subplans = Graphs.sortPostOrder(Planning.toDependencyGraph(plan)).toSeq

    val subplanCompilers = SubPlanCompiler(jpContext.getClassLoader)

    val builder = new SparkClientClassBuilder(source.getFlowId) {

      override def defMethods(methodDef: MethodDef): Unit = {
        methodDef.newMethod("execute", Type.INT_TYPE, Seq(classOf[SparkContext].asType)) { mb =>
          import mb._
          val scVar = `var`(classOf[SparkContext].asType, thisVar.nextLocal)
          val nextLocal = new AtomicInteger(scVar.nextLocal)
          val rddVars = mutable.Map.empty[Long, Var] // MarkerOperator.serialNumber

          subplans.foreach { subplan =>
            val dominant = subplan.getAttribute(classOf[DominantOperator]).getDominantOperator
            val compiler = subplanCompilers(dominant)
            val instantiator = compiler.instantiator
            val context = compiler.Context(source.getFlowId, jpContext)

            val subplanType = compiler.compile(subplan)(context)
            val driverVar = instantiator.newInstance(subplanType, subplan)(
              instantiator.Context(mb, scVar, rddVars, nextLocal, source.getFlowId, jpContext))
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
          // TODO Generate KryoRegistrator
          `return`(ldc("com.asakusafw.spark.runtime.serializer.KryoRegistrator"))
        }
      }
    }

    val client = jpContext.addClass(builder)

    jpContext.addTask(ModuleName, ProfileName, Command,
      Seq(CommandToken.BATCH_ID, CommandToken.FLOW_ID, CommandToken.EXECUTION_ID, CommandToken.BATCH_ARGUMENTS,
        CommandToken.of(client.getClassName)))
  }

  def preparePlan(graph: OperatorGraph): Plan = {
    val plan = new LogicalSparkPlanner().createPlan(graph).getPlan

    if (Logger.isDebugEnabled) {
      val dotGenerator = new DotGenerator
      val dot = dotGenerator.generate(plan, "simple")
      val str = new StringWriter()
      val writer = new PrintWriter(str)
      try {
        dotGenerator.save(writer, dot)
      } finally {
        writer.close()
      }
      Logger.debug(str.toString)
    }

    plan
  }
}

object SparkClientCompiler {

  val ModuleName: String = "spark"

  val ProfileName: String = "spark"

  val Command: Location = Location.of("spark/bin/spark-execute.sh")
}
