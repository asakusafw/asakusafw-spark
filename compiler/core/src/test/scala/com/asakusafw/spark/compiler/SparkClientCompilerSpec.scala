package com.asakusafw.spark.compiler

import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner

import java.io.{ File, DataInput, DataOutput }
import java.net.URLClassLoader
import java.util.{ List => JList }

import scala.collection.JavaConversions._

import org.apache.hadoop.io.{ NullWritable, Writable }
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.SparkContext._

import com.asakusafw.bridge.stage.StageInfo
import com.asakusafw.lang.compiler.api.CompilerOptions
import com.asakusafw.lang.compiler.api.testing.MockJobflowProcessorContext
import com.asakusafw.lang.compiler.model.PropertyName
import com.asakusafw.lang.compiler.model.description.ClassDescription
import com.asakusafw.lang.compiler.model.graph._
import com.asakusafw.lang.compiler.model.info.ExternalInputInfo
import com.asakusafw.lang.compiler.model.testing.OperatorExtractor
import com.asakusafw.lang.compiler.planning._
import com.asakusafw.lang.compiler.planning.spark.{ DominantOperator, PartitioningParameters }
import com.asakusafw.runtime.core.Result
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.runtime.value._
import com.asakusafw.runtime.stage.StageConstants
import com.asakusafw.runtime.stage.input.TemporaryInputFormat
import com.asakusafw.runtime.stage.output.TemporaryOutputFormat
import com.asakusafw.spark.runtime._
import com.asakusafw.spark.tools.asm._
import com.asakusafw.utils.graph.Graphs
import com.asakusafw.vocabulary.operator._

@RunWith(classOf[JUnitRunner])
class SparkClientCompilerSpecTest extends SparkClientCompilerSpec

class SparkClientCompilerSpec extends FlatSpec with LoadClassSugar {

  import SparkClientCompilerSpec._

  behavior of classOf[SparkClientCompiler].getSimpleName

  it should "compile Spark client from simple plan" in {
    val tmpDir = File.createTempFile("test-", null)
    tmpDir.delete
    val classpath = new File(tmpDir, "classes").getAbsoluteFile
    classpath.mkdirs()
    val path = new File(tmpDir, "tmp").getAbsolutePath

    spark { sc =>
      val hoges = sc.parallelize(0 until 100).map { i =>
        val hoge = new Hoge()
        hoge.id.modify(i)
        hoge
      }
      val job = Job.getInstance(sc.hadoopConfiguration)
      job.setOutputKeyClass(classOf[NullWritable])
      job.setOutputValueClass(classOf[Hoge])
      job.setOutputFormatClass(classOf[TemporaryOutputFormat[Hoge]])
      TemporaryOutputFormat.setOutputPath(job, new Path(path, "extenal/input/hoge"))
      hoges.map((NullWritable.get, _)).saveAsNewAPIHadoopDataset(job.getConfiguration)
    }

    val beginMarker = MarkerOperator.builder(ClassDescription.of(classOf[Hoge]))
      .attribute(classOf[PlanMarker], PlanMarker.BEGIN).build()

    val inputOperator = ExternalInput.builder("hoge/part-*",
      new ExternalInputInfo.Basic(
        ClassDescription.of(classOf[Hoge]),
        "test",
        ClassDescription.of(classOf[Hoge]),
        ExternalInputInfo.DataSize.UNKNOWN))
      .input("begin", ClassDescription.of(classOf[Hoge]), beginMarker.getOutput)
      .output(ExternalInput.PORT_NAME, ClassDescription.of(classOf[Hoge]))
      .constraint(OperatorConstraint.GENERATOR).build()

    val checkpointMarker = MarkerOperator.builder(ClassDescription.of(classOf[Hoge]))
      .attribute(classOf[PlanMarker], PlanMarker.CHECKPOINT).build()
    inputOperator.findOutput(ExternalInput.PORT_NAME).connect(checkpointMarker.getInput)

    val outputOperator = ExternalOutput.builder("output")
      .input(ExternalOutput.PORT_NAME, ClassDescription.of(classOf[Hoge]), checkpointMarker.getOutput)
      .output("end", ClassDescription.of(classOf[Hoge]))
      .constraint(OperatorConstraint.AT_LEAST_ONCE).build()

    val endMarker = MarkerOperator.builder(ClassDescription.of(classOf[Hoge]))
      .attribute(classOf[PlanMarker], PlanMarker.END).build()
    outputOperator.findOutput("end").connect(endMarker.getInput)

    val graph = new OperatorGraph(Seq(beginMarker, inputOperator, checkpointMarker, outputOperator, endMarker))

    val compiler = new SparkClientCompiler {

      override def preparePlan(graph: OperatorGraph, flowId: String): Plan = {
        val plan = PlanBuilder.from(graph.getOperators)
          .add(
            Seq(graph.getOperators.find(_.getOriginalSerialNumber == beginMarker.getOriginalSerialNumber).get),
            Seq(graph.getOperators.find(_.getOriginalSerialNumber == checkpointMarker.getOriginalSerialNumber).get))
          .add(
            Seq(graph.getOperators.find(_.getOriginalSerialNumber == checkpointMarker.getOriginalSerialNumber).get),
            Seq(graph.getOperators.find(_.getOriginalSerialNumber == endMarker.getOriginalSerialNumber).get))
          .build().getPlan
        assert(plan.getElements.size === 2)
        val sorted = Graphs.sortPostOrder(Planning.toDependencyGraph(plan))
        sorted(0).putAttribute(classOf[DominantOperator], new DominantOperator(inputOperator))
        sorted(1).putAttribute(classOf[DominantOperator], new DominantOperator(outputOperator))
        plan
      }
    }

    val jpContext = new MockJobflowProcessorContext(
      new CompilerOptions("buildid", path, Map.empty[String, String]),
      Thread.currentThread.getContextClassLoader,
      classpath)
    val jobflow = new Jobflow("flowId", ClassDescription.of(classOf[SparkClientCompilerSpec]), graph)

    compiler.process(jpContext, jobflow)

    val cl = Thread.currentThread.getContextClassLoader
    try {
      val classloader = new URLClassLoader(Array(classpath.toURI.toURL), cl)
      Thread.currentThread.setContextClassLoader(classloader)
      val cls = Class.forName("com.asakusafw.generated.spark.flowId.SparkClient", true, classloader)
        .asSubclass(classOf[SparkClient])
      val instance = cls.newInstance

      val conf = new SparkConf()
      conf.setAppName("AsakusaSparkClient")
      conf.setMaster("local[*]")

      val stageInfo = new StageInfo(
        sys.props("user.name"), "batchId", "flowId", null, "executionId", Map.empty[String, String])
      conf.setHadoopConf(Props.StageInfo, stageInfo.serialize)

      instance.execute(conf)
    } finally {
      Thread.currentThread.setContextClassLoader(cl)
    }

    spark { sc =>
      val job = Job.getInstance(sc.hadoopConfiguration)
      TemporaryInputFormat.setInputPaths(job, Seq(new Path(path, s"output/${outputOperator.getSerialNumber}/part-*")))
      val rdd = sc.newAPIHadoopRDD(
        job.getConfiguration,
        classOf[TemporaryInputFormat[Hoge]],
        classOf[NullWritable],
        classOf[Hoge])
      assert(rdd.map(_._2.id.get).collect === (0 until 100))
    }
  }

  it should "compile Spark client with Extract" in {
    val tmpDir = File.createTempFile("test-", null)
    tmpDir.delete
    val classpath = new File(tmpDir, "classes").getAbsoluteFile
    classpath.mkdirs()
    val path = new File(tmpDir, "tmp").getAbsolutePath

    spark { sc =>
      val hoges = sc.parallelize(0 until 100).map { i =>
        val hoge = new Hoge()
        hoge.id.modify(i)
        hoge
      }
      val job = Job.getInstance(sc.hadoopConfiguration)
      job.setOutputKeyClass(classOf[NullWritable])
      job.setOutputValueClass(classOf[Hoge])
      job.setOutputFormatClass(classOf[TemporaryOutputFormat[Hoge]])
      TemporaryOutputFormat.setOutputPath(job, new Path(path, "extenal/input/hoge"))
      hoges.map((NullWritable.get, _)).saveAsNewAPIHadoopDataset(job.getConfiguration)
    }

    val beginMarker = MarkerOperator.builder(ClassDescription.of(classOf[Hoge]))
      .attribute(classOf[PlanMarker], PlanMarker.BEGIN).build()

    val inputOperator = ExternalInput.builder("hoge/part-*",
      new ExternalInputInfo.Basic(
        ClassDescription.of(classOf[Hoge]),
        "test",
        ClassDescription.of(classOf[Hoge]),
        ExternalInputInfo.DataSize.UNKNOWN))
      .input("begin", ClassDescription.of(classOf[Hoge]), beginMarker.getOutput)
      .output(ExternalInput.PORT_NAME, ClassDescription.of(classOf[Hoge]))
      .constraint(OperatorConstraint.GENERATOR).build()

    val cpMarker = MarkerOperator.builder(ClassDescription.of(classOf[Hoge]))
      .attribute(classOf[PlanMarker], PlanMarker.CHECKPOINT).build()
    inputOperator.findOutput(ExternalInput.PORT_NAME).connect(cpMarker.getInput)

    val extractOperator = OperatorExtractor
      .extract(classOf[Extract], classOf[Ops], "extract")
      .input("hoge", ClassDescription.of(classOf[Hoge]), cpMarker.getOutput)
      .output("evenResult", ClassDescription.of(classOf[Hoge]))
      .output("oddResult", ClassDescription.of(classOf[Hoge]))
      .build()

    val evenMarker = MarkerOperator.builder(ClassDescription.of(classOf[Hoge]))
      .attribute(classOf[PlanMarker], PlanMarker.CHECKPOINT).build()
    extractOperator.findOutput("evenResult").connect(evenMarker.getInput)

    val evenOutputOperator = ExternalOutput.builder("even")
      .input(ExternalOutput.PORT_NAME, ClassDescription.of(classOf[Hoge]), evenMarker.getOutput)
      .output("end", ClassDescription.of(classOf[Hoge]))
      .constraint(OperatorConstraint.AT_LEAST_ONCE).build()

    val evenEndMarker = MarkerOperator.builder(ClassDescription.of(classOf[Hoge]))
      .attribute(classOf[PlanMarker], PlanMarker.END).build()
    evenOutputOperator.findOutput("end").connect(evenEndMarker.getInput)

    val oddMarker = MarkerOperator.builder(ClassDescription.of(classOf[Hoge]))
      .attribute(classOf[PlanMarker], PlanMarker.CHECKPOINT).build()
    extractOperator.findOutput("oddResult").connect(oddMarker.getInput)

    val oddOutputOperator = ExternalOutput.builder("odd")
      .input(ExternalOutput.PORT_NAME, ClassDescription.of(classOf[Hoge]), oddMarker.getOutput)
      .output("end", ClassDescription.of(classOf[Hoge]))
      .constraint(OperatorConstraint.AT_LEAST_ONCE).build()

    val oddEndMarker = MarkerOperator.builder(ClassDescription.of(classOf[Hoge]))
      .attribute(classOf[PlanMarker], PlanMarker.END).build()
    oddOutputOperator.findOutput("end").connect(oddEndMarker.getInput)

    val graph = new OperatorGraph(Seq(beginMarker, inputOperator,
      cpMarker, extractOperator,
      evenMarker, evenOutputOperator, evenEndMarker,
      oddMarker, oddOutputOperator, oddEndMarker))

    val compiler = new SparkClientCompiler {

      override def preparePlan(graph: OperatorGraph, flowId: String): Plan = {
        val plan = PlanBuilder.from(graph.getOperators)
          .add(
            Seq(graph.getOperators.find(_.getOriginalSerialNumber == beginMarker.getOriginalSerialNumber).get),
            Seq(graph.getOperators.find(_.getOriginalSerialNumber == cpMarker.getOriginalSerialNumber).get))
          .add(
            Seq(graph.getOperators.find(_.getOriginalSerialNumber == cpMarker.getOriginalSerialNumber).get),
            Seq(
              graph.getOperators.find(_.getOriginalSerialNumber == evenMarker.getOriginalSerialNumber).get,
              graph.getOperators.find(_.getOriginalSerialNumber == oddMarker.getOriginalSerialNumber).get))
          .add(
            Seq(graph.getOperators.find(_.getOriginalSerialNumber == evenMarker.getOriginalSerialNumber).get),
            Seq(graph.getOperators.find(_.getOriginalSerialNumber == evenEndMarker.getOriginalSerialNumber).get))
          .add(
            Seq(graph.getOperators.find(_.getOriginalSerialNumber == oddMarker.getOriginalSerialNumber).get),
            Seq(graph.getOperators.find(_.getOriginalSerialNumber == oddEndMarker.getOriginalSerialNumber).get))
          .build().getPlan
        assert(plan.getElements.size === 4)
        Seq(inputOperator, extractOperator,
          oddOutputOperator, evenOutputOperator).foreach { op =>
            plan.getElements.foreach { subplan =>
              if (subplan.getOperators.find(_.getOriginalSerialNumber == op.getOriginalSerialNumber).isDefined) {
                subplan.putAttribute(classOf[DominantOperator], new DominantOperator(op))
              }
            }
          }
        plan
      }
    }

    val jpContext = new MockJobflowProcessorContext(
      new CompilerOptions("buildid", path, Map.empty[String, String]),
      Thread.currentThread.getContextClassLoader,
      classpath)
    val jobflow = new Jobflow("flowId", ClassDescription.of(classOf[SparkClientCompilerSpec]), graph)

    compiler.process(jpContext, jobflow)

    val cl = Thread.currentThread.getContextClassLoader
    try {
      val classloader = new URLClassLoader(Array(classpath.toURI.toURL), cl)
      Thread.currentThread.setContextClassLoader(classloader)
      val cls = Class.forName("com.asakusafw.generated.spark.flowId.SparkClient", true, classloader)
        .asSubclass(classOf[SparkClient])
      val instance = cls.newInstance

      val conf = new SparkConf()
      conf.setAppName("AsakusaSparkClient")
      conf.setMaster("local[*]")

      val stageInfo = new StageInfo(
        sys.props("user.name"), "batchId", "flowId", null, "executionId", Map.empty[String, String])
      conf.setHadoopConf(Props.StageInfo, stageInfo.serialize)

      instance.execute(conf)
    } finally {
      Thread.currentThread.setContextClassLoader(cl)
    }

    spark { sc =>
      {
        val job = Job.getInstance(sc.hadoopConfiguration)
        TemporaryInputFormat.setInputPaths(job, Seq(new Path(path, s"even/${evenOutputOperator.getSerialNumber}/part-*")))
        val rdd = sc.newAPIHadoopRDD(
          job.getConfiguration,
          classOf[TemporaryInputFormat[Hoge]],
          classOf[NullWritable],
          classOf[Hoge])
        assert(rdd.map(_._2.id.get).collect === (0 until 100).filter(_ % 2 == 0))
      }
      {
        val job = Job.getInstance(sc.hadoopConfiguration)
        TemporaryInputFormat.setInputPaths(job, Seq(new Path(path, s"odd/${oddOutputOperator.getSerialNumber}/part-*")))
        val rdd = sc.newAPIHadoopRDD(
          job.getConfiguration,
          classOf[TemporaryInputFormat[Hoge]],
          classOf[NullWritable],
          classOf[Hoge])
        assert(rdd.map(_._2.id.get).collect === (0 until 100).filterNot(_ % 2 == 0))
      }
    }
  }

  it should "compile Spark client with CoGroup" in {
    val tmpDir = File.createTempFile("test-", null)
    tmpDir.delete
    val classpath = new File(tmpDir, "classes").getAbsoluteFile
    classpath.mkdirs()
    val path = new File(tmpDir, "tmp").getAbsolutePath

    spark { sc =>
      {
        val hoges = sc.parallelize(0 until 5).map { i =>
          val hoge = new Hoge()
          hoge.id.modify(i)
          hoge
        }
        val job = Job.getInstance(sc.hadoopConfiguration)
        job.setOutputKeyClass(classOf[NullWritable])
        job.setOutputValueClass(classOf[Hoge])
        job.setOutputFormatClass(classOf[TemporaryOutputFormat[Hoge]])
        TemporaryOutputFormat.setOutputPath(job, new Path(path, "extenal/input/hoge1"))
        hoges.map((NullWritable.get, _)).saveAsNewAPIHadoopDataset(job.getConfiguration)
      }
      {
        val hoges = sc.parallelize(5 until 10).map { i =>
          val hoge = new Hoge()
          hoge.id.modify(i)
          hoge
        }
        val job = Job.getInstance(sc.hadoopConfiguration)
        job.setOutputKeyClass(classOf[NullWritable])
        job.setOutputValueClass(classOf[Hoge])
        job.setOutputFormatClass(classOf[TemporaryOutputFormat[Hoge]])
        TemporaryOutputFormat.setOutputPath(job, new Path(path, "extenal/input/hoge2"))
        hoges.map((NullWritable.get, _)).saveAsNewAPIHadoopDataset(job.getConfiguration)
      }
      {
        val foos = sc.parallelize(0 until 10).flatMap(i => (0 until i).map { j =>
          val foo = new Foo()
          foo.id.modify(10 + j)
          foo.hogeId.modify(i)
          foo
        })
        val job = Job.getInstance(sc.hadoopConfiguration)
        job.setOutputKeyClass(classOf[NullWritable])
        job.setOutputValueClass(classOf[Foo])
        job.setOutputFormatClass(classOf[TemporaryOutputFormat[Hoge]])
        TemporaryOutputFormat.setOutputPath(job, new Path(path, "extenal/input/foo"))
        foos.map((NullWritable.get, _)).saveAsNewAPIHadoopDataset(job.getConfiguration)
      }
    }

    val hoge1BeginMarker = MarkerOperator.builder(ClassDescription.of(classOf[Hoge]))
      .attribute(classOf[PlanMarker], PlanMarker.BEGIN).build()

    val hoge1InputOperator = ExternalInput.builder("hoge1/part-*",
      new ExternalInputInfo.Basic(
        ClassDescription.of(classOf[Hoge]),
        "hoges1",
        ClassDescription.of(classOf[Hoge]),
        ExternalInputInfo.DataSize.UNKNOWN))
      .input("begin1", ClassDescription.of(classOf[Hoge]), hoge1BeginMarker.getOutput)
      .output(ExternalInput.PORT_NAME, ClassDescription.of(classOf[Hoge]))
      .constraint(OperatorConstraint.GENERATOR).build()

    val hoge1CpMarker = MarkerOperator.builder(ClassDescription.of(classOf[Hoge]))
      .attribute(classOf[PlanMarker], PlanMarker.CHECKPOINT).build()
    hoge1InputOperator.findOutput(ExternalInput.PORT_NAME).connect(hoge1CpMarker.getInput)

    val hoge2BeginMarker = MarkerOperator.builder(ClassDescription.of(classOf[Hoge]))
      .attribute(classOf[PlanMarker], PlanMarker.BEGIN).build()

    val hoge2InputOperator = ExternalInput.builder("hoge2/part-*",
      new ExternalInputInfo.Basic(
        ClassDescription.of(classOf[Hoge]),
        "hoges2",
        ClassDescription.of(classOf[Hoge]),
        ExternalInputInfo.DataSize.UNKNOWN))
      .input("begin2", ClassDescription.of(classOf[Hoge]), hoge2BeginMarker.getOutput)
      .output(ExternalInput.PORT_NAME, ClassDescription.of(classOf[Hoge]))
      .constraint(OperatorConstraint.GENERATOR).build()

    val hoge2CpMarker = MarkerOperator.builder(ClassDescription.of(classOf[Hoge]))
      .attribute(classOf[PlanMarker], PlanMarker.CHECKPOINT).build()
    hoge2InputOperator.findOutput(ExternalInput.PORT_NAME).connect(hoge2CpMarker.getInput)

    val fooBeginMarker = MarkerOperator.builder(ClassDescription.of(classOf[Foo]))
      .attribute(classOf[PlanMarker], PlanMarker.BEGIN).build()

    val fooInputOperator = ExternalInput.builder("foo/part-*",
      new ExternalInputInfo.Basic(
        ClassDescription.of(classOf[Foo]),
        "foos",
        ClassDescription.of(classOf[Foo]),
        ExternalInputInfo.DataSize.UNKNOWN))
      .input("begin", ClassDescription.of(classOf[Foo]), fooBeginMarker.getOutput)
      .output(ExternalInput.PORT_NAME, ClassDescription.of(classOf[Foo]))
      .constraint(OperatorConstraint.GENERATOR).build()

    val fooCpMarker = MarkerOperator.builder(ClassDescription.of(classOf[Foo]))
      .attribute(classOf[PlanMarker], PlanMarker.CHECKPOINT).build()
    fooInputOperator.findOutput(ExternalInput.PORT_NAME).connect(fooCpMarker.getInput)

    val cogroupOperator = OperatorExtractor
      .extract(classOf[CoGroup], classOf[Ops], "cogroup")
      .input("hoges", ClassDescription.of(classOf[Hoge]),
        new Group(Seq(PropertyName.of("id")), Seq.empty[Group.Ordering]),
        hoge1CpMarker.getOutput, hoge2CpMarker.getOutput)
      .input("foos", ClassDescription.of(classOf[Foo]),
        new Group(
          Seq(PropertyName.of("hogeId")),
          Seq(new Group.Ordering(PropertyName.of("id"), Group.Direction.ASCENDANT))),
        fooCpMarker.getOutput)
      .output("hogeResult", ClassDescription.of(classOf[Hoge]))
      .output("fooResult", ClassDescription.of(classOf[Foo]))
      .output("hogeError", ClassDescription.of(classOf[Hoge]))
      .output("fooError", ClassDescription.of(classOf[Foo]))
      .build()

    val hogeResultMarker = MarkerOperator.builder(ClassDescription.of(classOf[Hoge]))
      .attribute(classOf[PlanMarker], PlanMarker.CHECKPOINT).build()
    cogroupOperator.findOutput("hogeResult").connect(hogeResultMarker.getInput)

    val hogeResultOutputOperator = ExternalOutput.builder("hogeResult")
      .input(ExternalOutput.PORT_NAME, ClassDescription.of(classOf[Hoge]), hogeResultMarker.getOutput)
      .output("end", ClassDescription.of(classOf[Hoge]))
      .constraint(OperatorConstraint.AT_LEAST_ONCE).build()

    val hogeResultEndMarker = MarkerOperator.builder(ClassDescription.of(classOf[Hoge]))
      .attribute(classOf[PlanMarker], PlanMarker.END).build()
    hogeResultOutputOperator.findOutput("end").connect(hogeResultEndMarker.getInput)

    val fooResultMarker = MarkerOperator.builder(ClassDescription.of(classOf[Foo]))
      .attribute(classOf[PlanMarker], PlanMarker.CHECKPOINT).build()
    cogroupOperator.findOutput("fooResult").connect(fooResultMarker.getInput)

    val fooResultOutputOperator = ExternalOutput.builder("fooResult")
      .input(ExternalOutput.PORT_NAME, ClassDescription.of(classOf[Foo]), fooResultMarker.getOutput)
      .output("end", ClassDescription.of(classOf[Foo]))
      .constraint(OperatorConstraint.AT_LEAST_ONCE).build()

    val fooResultEndMarker = MarkerOperator.builder(ClassDescription.of(classOf[Foo]))
      .attribute(classOf[PlanMarker], PlanMarker.END).build()
    fooResultOutputOperator.findOutput("end").connect(fooResultEndMarker.getInput)

    val hogeErrorMarker = MarkerOperator.builder(ClassDescription.of(classOf[Hoge]))
      .attribute(classOf[PlanMarker], PlanMarker.CHECKPOINT).build()
    cogroupOperator.findOutput("hogeError").connect(hogeErrorMarker.getInput)

    val hogeErrorOutputOperator = ExternalOutput.builder("hogeError")
      .input(ExternalOutput.PORT_NAME, ClassDescription.of(classOf[Hoge]), hogeErrorMarker.getOutput)
      .output("end", ClassDescription.of(classOf[Hoge]))
      .constraint(OperatorConstraint.AT_LEAST_ONCE).build()

    val hogeErrorEndMarker = MarkerOperator.builder(ClassDescription.of(classOf[Hoge]))
      .attribute(classOf[PlanMarker], PlanMarker.END).build()
    hogeErrorOutputOperator.findOutput("end").connect(hogeErrorEndMarker.getInput)

    val fooErrorMarker = MarkerOperator.builder(ClassDescription.of(classOf[Foo]))
      .attribute(classOf[PlanMarker], PlanMarker.CHECKPOINT).build()
    cogroupOperator.findOutput("fooError").connect(fooErrorMarker.getInput)

    val fooErrorOutputOperator = ExternalOutput.builder("fooError")
      .input(ExternalOutput.PORT_NAME, ClassDescription.of(classOf[Foo]), fooErrorMarker.getOutput)
      .output("end", ClassDescription.of(classOf[Foo]))
      .constraint(OperatorConstraint.AT_LEAST_ONCE).build()

    val fooErrorEndMarker = MarkerOperator.builder(ClassDescription.of(classOf[Foo]))
      .attribute(classOf[PlanMarker], PlanMarker.END).build()
    fooErrorOutputOperator.findOutput("end").connect(fooErrorEndMarker.getInput)

    val graph = new OperatorGraph(Seq(
      hoge1BeginMarker, hoge1InputOperator, hoge1CpMarker,
      hoge2BeginMarker, hoge2InputOperator, hoge2CpMarker,
      fooBeginMarker, fooInputOperator, fooCpMarker,
      cogroupOperator,
      hogeResultMarker, hogeResultOutputOperator, hogeResultEndMarker,
      fooResultMarker, fooResultOutputOperator, fooResultEndMarker,
      hogeErrorMarker, hogeErrorOutputOperator, hogeErrorEndMarker,
      fooErrorMarker, fooErrorOutputOperator, fooErrorEndMarker))

    val compiler = new SparkClientCompiler {

      override def preparePlan(graph: OperatorGraph, flowId: String): Plan = {
        val plan = PlanBuilder.from(graph.getOperators)
          .add(
            Seq(graph.getOperators.find(_.getOriginalSerialNumber == hoge1BeginMarker.getOriginalSerialNumber).get),
            Seq(graph.getOperators.find(_.getOriginalSerialNumber == hoge1CpMarker.getOriginalSerialNumber).get))
          .add(
            Seq(graph.getOperators.find(_.getOriginalSerialNumber == hoge2BeginMarker.getOriginalSerialNumber).get),
            Seq(graph.getOperators.find(_.getOriginalSerialNumber == hoge2CpMarker.getOriginalSerialNumber).get))
          .add(
            Seq(graph.getOperators.find(_.getOriginalSerialNumber == fooBeginMarker.getOriginalSerialNumber).get),
            Seq(graph.getOperators.find(_.getOriginalSerialNumber == fooCpMarker.getOriginalSerialNumber).get))
          .add(
            Seq(
              graph.getOperators.find(_.getOriginalSerialNumber == hoge1CpMarker.getOriginalSerialNumber).get,
              graph.getOperators.find(_.getOriginalSerialNumber == hoge2CpMarker.getOriginalSerialNumber).get,
              graph.getOperators.find(_.getOriginalSerialNumber == fooCpMarker.getOriginalSerialNumber).get),
            Seq(
              graph.getOperators.find(_.getOriginalSerialNumber == hogeResultMarker.getOriginalSerialNumber).get,
              graph.getOperators.find(_.getOriginalSerialNumber == fooResultMarker.getOriginalSerialNumber).get,
              graph.getOperators.find(_.getOriginalSerialNumber == hogeErrorMarker.getOriginalSerialNumber).get,
              graph.getOperators.find(_.getOriginalSerialNumber == fooErrorMarker.getOriginalSerialNumber).get))
          .add(
            Seq(graph.getOperators.find(_.getOriginalSerialNumber == hogeResultMarker.getOriginalSerialNumber).get),
            Seq(graph.getOperators.find(_.getOriginalSerialNumber == hogeResultEndMarker.getOriginalSerialNumber).get))
          .add(
            Seq(graph.getOperators.find(_.getOriginalSerialNumber == fooResultMarker.getOriginalSerialNumber).get),
            Seq(graph.getOperators.find(_.getOriginalSerialNumber == fooResultEndMarker.getOriginalSerialNumber).get))
          .add(
            Seq(graph.getOperators.find(_.getOriginalSerialNumber == hogeErrorMarker.getOriginalSerialNumber).get),
            Seq(graph.getOperators.find(_.getOriginalSerialNumber == hogeErrorEndMarker.getOriginalSerialNumber).get))
          .add(
            Seq(graph.getOperators.find(_.getOriginalSerialNumber == fooErrorMarker.getOriginalSerialNumber).get),
            Seq(graph.getOperators.find(_.getOriginalSerialNumber == fooErrorEndMarker.getOriginalSerialNumber).get))
          .build().getPlan
        assert(plan.getElements.size === 8)
        Seq(hoge1InputOperator, hoge2InputOperator, fooInputOperator, cogroupOperator,
          hogeResultOutputOperator, hogeErrorOutputOperator,
          fooResultOutputOperator, fooErrorOutputOperator).foreach { op =>
            plan.getElements.foreach { subplan =>
              if (subplan.getOperators.find(_.getOriginalSerialNumber == op.getOriginalSerialNumber).isDefined) {
                subplan.putAttribute(classOf[DominantOperator], new DominantOperator(op))
              }
            }
          }

        plan.getElements.toSeq.find(_.getOperators.exists(_.getOriginalSerialNumber == hoge1CpMarker.getOriginalSerialNumber))
          .get
          .getOutputs.foreach { output =>
            output.putAttribute(classOf[PartitioningParameters],
              new PartitioningParameters(new Group(Seq(PropertyName.of("id")), Seq.empty[Group.Ordering])))
          }
        plan.getElements.find(_.getOperators.exists(_.getOriginalSerialNumber == hoge2CpMarker.getOriginalSerialNumber))
          .get
          .getOutputs.foreach { output =>
            output.putAttribute(classOf[PartitioningParameters],
              new PartitioningParameters(new Group(Seq(PropertyName.of("id")), Seq.empty[Group.Ordering])))
          }
        plan.getElements.find(_.getOperators.exists(_.getOriginalSerialNumber == fooCpMarker.getOriginalSerialNumber))
          .get
          .getOutputs.foreach { output =>
            output.putAttribute(classOf[PartitioningParameters],
              new PartitioningParameters(
                new Group(
                  Seq(PropertyName.of("hogeId")),
                  Seq(new Group.Ordering(PropertyName.of("id"), Group.Direction.ASCENDANT)))))
          }

        plan
      }
    }

    val jpContext = new MockJobflowProcessorContext(
      new CompilerOptions("buildid", path, Map.empty[String, String]),
      Thread.currentThread.getContextClassLoader,
      classpath)
    val jobflow = new Jobflow("flowId", ClassDescription.of(classOf[SparkClientCompilerSpec]), graph)

    compiler.process(jpContext, jobflow)

    val cl = Thread.currentThread.getContextClassLoader
    try {
      val classloader = new URLClassLoader(Array(classpath.toURI.toURL), cl)
      Thread.currentThread.setContextClassLoader(classloader)
      val cls = Class.forName("com.asakusafw.generated.spark.flowId.SparkClient", true, classloader)
        .asSubclass(classOf[SparkClient])
      val instance = cls.newInstance

      val conf = new SparkConf()
      conf.setAppName("AsakusaSparkClient")
      conf.setMaster("local[*]")

      val stageInfo = new StageInfo(
        sys.props("user.name"), "batchId", "flowId", null, "executionId", Map.empty[String, String])
      conf.setHadoopConf(Props.StageInfo, stageInfo.serialize)

      instance.execute(conf)
    } finally {
      Thread.currentThread.setContextClassLoader(cl)
    }

    spark { sc =>
      {
        val job = Job.getInstance(sc.hadoopConfiguration)
        TemporaryInputFormat.setInputPaths(job, Seq(new Path(path, s"hogeResult/${hogeResultOutputOperator.getSerialNumber}/part-*")))
        val hogeResult = sc.newAPIHadoopRDD(
          job.getConfiguration,
          classOf[TemporaryInputFormat[Hoge]],
          classOf[NullWritable],
          classOf[Hoge]).map(_._2.id.get).collect.toSeq
        assert(hogeResult.size === 1)
        assert(hogeResult(0) === 1)
      }
      {
        val job = Job.getInstance(sc.hadoopConfiguration)
        TemporaryInputFormat.setInputPaths(job, Seq(new Path(path, s"fooResult/${fooResultOutputOperator.getSerialNumber}/part-*")))
        val fooResult = sc.newAPIHadoopRDD(
          job.getConfiguration,
          classOf[TemporaryInputFormat[Foo]],
          classOf[NullWritable],
          classOf[Foo]).map(_._2).map(foo => (foo.id.get, foo.hogeId.get)).collect.toSeq
        assert(fooResult.size === 1)
        assert(fooResult(0) === (10, 1))
      }
      {
        val job = Job.getInstance(sc.hadoopConfiguration)
        TemporaryInputFormat.setInputPaths(job, Seq(new Path(path, s"hogeError/${hogeErrorOutputOperator.getSerialNumber}/part-*")))
        val hogeError = sc.newAPIHadoopRDD(
          job.getConfiguration,
          classOf[TemporaryInputFormat[Hoge]],
          classOf[NullWritable],
          classOf[Hoge]).map(_._2.id.get).collect.toSeq.sorted
        assert(hogeError.size === 9)
        assert(hogeError(0) === 0)
        for (i <- 2 until 10) {
          assert(hogeError(i - 1) === i)
        }
      }
      {
        val job = Job.getInstance(sc.hadoopConfiguration)
        TemporaryInputFormat.setInputPaths(job, Seq(new Path(path, s"fooError/${fooErrorOutputOperator.getSerialNumber}/part-*")))
        val fooError = sc.newAPIHadoopRDD(
          job.getConfiguration,
          classOf[TemporaryInputFormat[Foo]],
          classOf[NullWritable],
          classOf[Foo]).map(_._2).map(foo => (foo.id.get, foo.hogeId.get)).collect.toSeq
          .sortBy(foo => (foo._2, foo._1))
        assert(fooError.size === 44)
        for {
          i <- 2 until 10
          j <- 0 until i
        } {
          assert(fooError((i * (i - 1)) / 2 + j - 1) == (10 + j, i))
        }
      }
    }
  }

  def spark[A](block: SparkContext => A): A = {
    val sc = new SparkContext(new SparkConf().setAppName("").setMaster("local[*]"))
    try {
      block(sc)
    } finally {
      sc.stop
    }
  }
}

object SparkClientCompilerSpec {

  class Hoge extends DataModel[Hoge] with Writable {

    val id = new IntOption()

    override def reset(): Unit = {
      id.setNull()
    }
    override def copyFrom(other: Hoge): Unit = {
      id.copyFrom(other.id)
    }
    override def readFields(in: DataInput): Unit = {
      id.readFields(in)
    }
    override def write(out: DataOutput): Unit = {
      id.write(out)
    }

    def getIdOption: IntOption = id
  }

  class Foo extends DataModel[Foo] with Writable {

    val id = new IntOption()
    val hogeId = new IntOption()

    override def reset(): Unit = {
      id.setNull()
      hogeId.setNull()
    }
    override def copyFrom(other: Foo): Unit = {
      id.copyFrom(other.id)
      hogeId.copyFrom(other.hogeId)
    }
    override def readFields(in: DataInput): Unit = {
      id.readFields(in)
      hogeId.readFields(in)
    }
    override def write(out: DataOutput): Unit = {
      id.write(out)
      hogeId.write(out)
    }

    def getIdOption: IntOption = id
    def getHogeIdOption: IntOption = hogeId
  }

  class Ops {

    @Extract
    def extract(hoge: Hoge, evenResult: Result[Hoge], oddResult: Result[Hoge]): Unit = {
      if (hoge.id.get % 2 == 0) {
        evenResult.add(hoge)
      } else {
        oddResult.add(hoge)
      }
    }

    @CoGroup
    def cogroup(
      hogeList: JList[Hoge], fooList: JList[Foo],
      hogeResult: Result[Hoge], fooResult: Result[Foo],
      hogeError: Result[Hoge], fooError: Result[Foo]): Unit = {
      if (hogeList.size == 1 && fooList.size == 1) {
        hogeResult.add(hogeList(0))
        fooResult.add(fooList(0))
      } else {
        hogeList.foreach(hogeError.add)
        fooList.foreach(fooError.add)
      }
    }
  }
}
