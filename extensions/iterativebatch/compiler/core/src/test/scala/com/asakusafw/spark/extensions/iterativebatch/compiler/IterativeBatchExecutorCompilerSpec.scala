/*
 * Copyright 2011-2016 Asakusa Framework Team.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.asakusafw.spark.extensions.iterativebatch.compiler

import org.junit.runner.RunWith
import org.scalatest.Suites
import org.scalatest.fixture.FlatSpec
import org.scalatest.junit.JUnitRunner

import java.io.{ File, DataInput, DataOutput }
import java.util.{ List => JList }

import scala.collection.JavaConversions._
import scala.concurrent.ExecutionContext
import scala.reflect.{ classTag, ClassTag }

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{ NullWritable, Writable }
import org.apache.hadoop.mapreduce.{ Job => MRJob }
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.{ FileOutputFormat, SequenceFileOutputFormat }
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.rdd.RDD
import org.objectweb.asm.Type

import com.asakusafw.bridge.api.BatchContext
import com.asakusafw.bridge.hadoop.directio.DirectFileInputFormat
import com.asakusafw.bridge.stage.StageInfo
import com.asakusafw.lang.compiler.api.CompilerOptions
import com.asakusafw.lang.compiler.api.testing.MockJobflowProcessorContext
import com.asakusafw.lang.compiler.common.Location
import com.asakusafw.lang.compiler.hadoop.{ InputFormatInfo, InputFormatInfoExtension }
import com.asakusafw.lang.compiler.inspection.{ AbstractInspectionExtension, InspectionExtension }
import com.asakusafw.lang.compiler.model.description.ClassDescription
import com.asakusafw.lang.compiler.model.graph._
import com.asakusafw.lang.compiler.model.info.ExternalInputInfo
import com.asakusafw.lang.compiler.model.iterative.IterativeExtension
import com.asakusafw.lang.compiler.model.testing.OperatorExtractor
import com.asakusafw.lang.compiler.planning._
import com.asakusafw.runtime.core.Result
import com.asakusafw.runtime.directio.hadoop.{ HadoopDataSource, SequenceFileFormat }
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.runtime.stage.input.TemporaryInputFormat
import com.asakusafw.runtime.stage.output.TemporaryOutputFormat
import com.asakusafw.runtime.value._
import com.asakusafw.spark.compiler.{ ClassServerForAll, FlowIdForEach }
import com.asakusafw.spark.compiler.planning.SparkPlanning
import com.asakusafw.spark.runtime._
import com.asakusafw.spark.runtime.fixture.SparkForAll
import com.asakusafw.spark.tools.asm._
import com.asakusafw.vocabulary.flow.processor.PartialAggregation
import com.asakusafw.vocabulary.model.{ Key, Joined, Summarized }
import com.asakusafw.vocabulary.operator._

import com.asakusafw.spark.extensions.iterativebatch.runtime.IterativeBatchExecutor

@RunWith(classOf[JUnitRunner])
class IterativeBatchExecutorCompilerSpecTest extends IterativeBatchExecutorCompilerSpec

class IterativeBatchExecutorCompilerSpec extends Suites(
  (for {
    threshold <- Seq(None, Some(0))
    parallelism <- Seq(None, Some(8), Some(0))
  } yield {
    new IterativeBatchExecutorCompilerSpecBase(threshold, parallelism)
  }): _*)

class IterativeBatchExecutorCompilerSpecBase(threshold: Option[Int], parallelism: Option[Int])
  extends FlatSpec
  with ClassServerForAll
  with SparkForAll
  with FlowIdForEach
  with TempDirForEach
  with UsingCompilerContext
  with RoundContextSugar {

  import IterativeBatchExecutorCompilerSpec._

  behavior of IterativeBatchExecutorCompiler.getClass.getSimpleName

  val configuration =
    "master=local[8]" +
      s"${threshold.map(t => s", threshold=${t}").getOrElse("")}" +
      s"${parallelism.map(p => s", parallelism=${p}").getOrElse("")}"

  override def configure(conf: SparkConf): SparkConf = {
    threshold.foreach(i => conf.set("spark.shuffle.sort.bypassMergeThreshold", i.toString))
    parallelism.foreach(para => conf.set(Props.Parallelism, para.toString))
    super.configure(conf)
  }

  val configurePath: (MRJob, File, String) => Unit = { (job, path, name) =>
    job.setOutputFormatClass(classOf[TemporaryOutputFormat[_]])
    TemporaryOutputFormat.setOutputPath(
      job,
      new Path(path.getPath, s"${MockJobflowProcessorContext.EXTERNAL_INPUT_BASE}${name}"))
  }

  def prepareData[T: ClassTag](
    name: String,
    path: File,
    configurePath: (MRJob, File, String) => Unit = configurePath)(
      rdd: RDD[T])(
        implicit sc: SparkContext): Unit = {
    val job = MRJob.getInstance(sc.hadoopConfiguration)
    job.setOutputKeyClass(classOf[NullWritable])
    job.setOutputValueClass(classTag[T].runtimeClass)
    configurePath(job, path, name)
    rdd.map((NullWritable.get, _)).saveAsNewAPIHadoopDataset(job.getConfiguration)
  }

  def readResult[T: ClassTag](name: String, round: Int, path: File)(implicit sc: SparkContext): RDD[T] = {
    val job = MRJob.getInstance(sc.hadoopConfiguration)
    TemporaryInputFormat.setInputPaths(
      job,
      Seq(new Path(path.getPath, s"${name}/round_${round}/part-*")))
    sc.newAPIHadoopRDD(
      job.getConfiguration,
      classOf[TemporaryInputFormat[T]],
      classOf[NullWritable],
      classTag[T].runtimeClass.asInstanceOf[Class[T]]).map(_._2)
  }

  for {
    iterativeExtension <- Seq(
      new IterativeExtension(),
      new IterativeExtension("round"))
  } {
    val conf = s"${configuration}, IterativeExtension: ${iterativeExtension}"

    it should s"compile IterativeBatchExecutor from simple plan: [${conf}]" in { implicit sc =>
      val path = createTempDirectoryForEach("test-").toFile

      prepareData("foos", path) {
        sc.parallelize(0 until 100).map(Foo.intToFoo)
      }

      val inputOperator = ExternalInput
        .newInstance("foos/part-*",
          new ExternalInputInfo.Basic(
            ClassDescription.of(classOf[Foo]),
            "test",
            ClassDescription.of(classOf[Foo]),
            ExternalInputInfo.DataSize.UNKNOWN))

      val roundFoo = OperatorExtractor
        .extract(classOf[Update], classOf[Ops], "roundFoo")
        .input("foo", ClassDescription.of(classOf[Foo]), inputOperator.getOperatorPort)
        .output("output", ClassDescription.of(classOf[Foo]))
        .attribute(classOf[IterativeExtension], iterativeExtension)
        .build()

      val outputOperator = ExternalOutput
        .newInstance("output", roundFoo.findOutput("output"))

      val graph = new OperatorGraph(Seq(inputOperator, outputOperator))

      val executorType = compile(flowId, graph, 2, path, classServer.root.toFile)

      val rounds = 0 to 1
      execute(flowId, executorType, rounds)

      for {
        round <- rounds
      } {
        val result = readResult[Foo](outputOperator.getName, round, path)
          .map { foo =>
            (foo.id.get, foo.foo.getAsString)
          }.collect.toSeq.sortBy(_._1)
        assert(result === (0 until 100).map(i => (100 * round + i, s"foo${100 * round + i}")))
      }
    }
  }

  for {
    iterativeExtension <- Seq(
      new IterativeExtension(),
      new IterativeExtension("round"))
  } {
    val conf = s"${configuration}, IterativeExtension: ${iterativeExtension}"

    it should s"compile IterativeBatchExecutor from simple plan with InputFormatInfo: [${conf}]" in { implicit sc =>
      val path = createTempDirectoryForEach("test-").toFile

      val configurePath: (MRJob, File, String) => Unit = { (job, path, name) =>
        job.setOutputFormatClass(classOf[SequenceFileOutputFormat[NullWritable, Foo]])
        FileOutputFormat.setOutputPath(job, new Path(path.getPath, name))
      }

      val rounds = 0 to 1
      for {
        round <- rounds
      } {
        prepareData(s"foos_${round}", path, configurePath) {
          sc.parallelize(0 until 100).map(Foo.intToFoo).map(Foo.round(_, round))
        }
      }

      val inputOperator = ExternalInput
        .newWithAttributes("foos",
          new ExternalInputInfo.Basic(
            ClassDescription.of(classOf[Foo]),
            "test",
            ClassDescription.of(classOf[Foo]),
            ExternalInputInfo.DataSize.UNKNOWN))
        .attribute(classOf[IterativeExtension], iterativeExtension)
        .build()

      val outputOperator = ExternalOutput
        .newInstance("output", inputOperator.findOutput(ExternalInput.PORT_NAME))

      val graph = new OperatorGraph(Seq(inputOperator, outputOperator))

      val executorType = {
        val jpContext = newJPContext(path, classServer.root.toFile)
        jpContext.registerExtension(
          classOf[InputFormatInfoExtension],
          new InputFormatInfoExtension() {

            override def resolve(name: String, info: ExternalInputInfo): InputFormatInfo = {
              new InputFormatInfo(
                ClassDescription.of(classOf[DirectFileInputFormat]),
                ClassDescription.of(classOf[NullWritable]),
                ClassDescription.of(classOf[Foo]),
                Map(
                  "com.asakusafw.directio.test" -> classOf[HadoopDataSource].getName,
                  "com.asakusafw.directio.test.path" -> "test",
                  "com.asakusafw.directio.test.fs.path" -> path.getAbsolutePath,
                  DirectFileInputFormat.KEY_BASE_PATH -> "test",
                  DirectFileInputFormat.KEY_RESOURCE_PATH -> "foos_${round}/part-*",
                  DirectFileInputFormat.KEY_DATA_CLASS -> classOf[Foo].getName,
                  DirectFileInputFormat.KEY_FORMAT_CLASS -> classOf[FooSequenceFileFormat].getName))
            }
          })
        val jobflow = newJobflow(flowId, graph)
        val plan = SparkPlanning.plan(jpContext, jobflow).getPlan
        assert(plan.getElements.size === 2)

        implicit val context = newIterativeBatchExecutorCompilerContext(flowId, jpContext)
        val executorType = IterativeBatchExecutorCompiler.compile(plan)
        context.addClass(context.branchKeys)
        context.addClass(context.broadcastIds)
        executorType
      }

      execute(flowId, executorType, rounds)

      for {
        round <- rounds
      } {
        val result = readResult[Foo](outputOperator.getName, round, path)
          .map { foo =>
            (foo.id.get, foo.foo.getAsString)
          }.collect.toSeq.sortBy(_._1)
        assert(result === (0 until 100).map(i => (100 * round + i, s"foo${100 * round + i}")))
      }
    }
  }

  for {
    iterativeExtension <- Seq(
      new IterativeExtension(),
      new IterativeExtension("round"))
  } {
    val conf = s"${configuration}, IterativeExtension: ${iterativeExtension}"

    it should s"compile IterativeBatchExecutor with Logging: [${conf}]" in { implicit sc =>
      val path = createTempDirectoryForEach("test-").toFile

      prepareData("foos", path) {
        sc.parallelize(0 until 100).map(Foo.intToFoo)
      }

      val inputOperator = ExternalInput
        .newInstance("foos/part-*",
          new ExternalInputInfo.Basic(
            ClassDescription.of(classOf[Foo]),
            "test",
            ClassDescription.of(classOf[Foo]),
            ExternalInputInfo.DataSize.UNKNOWN))

      val roundFoo = OperatorExtractor
        .extract(classOf[Update], classOf[Ops], "roundFoo")
        .input("foo", ClassDescription.of(classOf[Foo]), inputOperator.getOperatorPort)
        .output("output", ClassDescription.of(classOf[Foo]))
        .attribute(classOf[IterativeExtension], iterativeExtension)
        .build()

      val loggingOperator = OperatorExtractor
        .extract(classOf[Logging], classOf[Ops], "logging")
        .input("foo", ClassDescription.of(classOf[Foo]), roundFoo.findOutput("output"))
        .output("output", ClassDescription.of(classOf[Foo]))
        .build()

      val outputOperator = ExternalOutput
        .newInstance("output", loggingOperator.findOutput("output"))

      val graph = new OperatorGraph(Seq(inputOperator, loggingOperator, outputOperator))

      val executorType = compile(flowId, graph, 2, path, classServer.root.toFile)

      val rounds = 0 to 1
      execute(flowId, executorType, rounds)

      for {
        round <- rounds
      } {
        val result = readResult[Foo](outputOperator.getName, round, path)
          .map { foo =>
            (foo.id.get, foo.foo.getAsString)
          }.collect.toSeq.sortBy(_._1)
        assert(result === (0 until 100).map(i => (100 * round + i, s"foo${100 * round + i}")))
      }
    }
  }

  for {
    iterativeExtension <- Seq(
      new IterativeExtension(),
      new IterativeExtension("round"))
  } {
    val conf = s"${configuration}, IterativeExtension: ${iterativeExtension}"

    it should s"compile IterativeBatchExecutor with Extract: [${conf}]" in { implicit sc =>
      val path = createTempDirectoryForEach("test-").toFile

      prepareData("foos", path) {
        sc.parallelize(0 until 100).map(Foo.intToFoo)
      }

      val inputOperator = ExternalInput
        .newInstance("foos/part-*",
          new ExternalInputInfo.Basic(
            ClassDescription.of(classOf[Foo]),
            "test",
            ClassDescription.of(classOf[Foo]),
            ExternalInputInfo.DataSize.UNKNOWN))

      val roundFoo = OperatorExtractor
        .extract(classOf[Update], classOf[Ops], "roundFoo")
        .input("foo", ClassDescription.of(classOf[Foo]), inputOperator.getOperatorPort)
        .output("output", ClassDescription.of(classOf[Foo]))
        .attribute(classOf[IterativeExtension], iterativeExtension)
        .build()

      val extractOperator = OperatorExtractor
        .extract(classOf[Extract], classOf[Ops], "extract")
        .input("foo", ClassDescription.of(classOf[Foo]), roundFoo.findOutput("output"))
        .output("evenResult", ClassDescription.of(classOf[Foo]))
        .output("oddResult", ClassDescription.of(classOf[Foo]))
        .build()

      val evenOutputOperator = ExternalOutput
        .newInstance("even", extractOperator.findOutput("evenResult"))

      val oddOutputOperator = ExternalOutput
        .newInstance("odd", extractOperator.findOutput("oddResult"))

      val graph = new OperatorGraph(Seq(
        inputOperator,
        extractOperator,
        evenOutputOperator,
        oddOutputOperator))

      val executorType = compile(flowId, graph, 3, path, classServer.root.toFile)

      val rounds = 0 to 1
      execute(flowId, executorType, rounds)

      for {
        round <- rounds
      } {
        {
          val result = readResult[Foo](evenOutputOperator.getName, round, path)
            .map { foo =>
              (foo.id.get, foo.foo.getAsString)
            }.collect.toSeq.sortBy(_._1)
          assert(result ===
            (0 until 100).filter(_ % 2 == 0).map(i => (100 * round + i, s"foo${100 * round + i}")))
        }
        {
          val result = readResult[Foo](oddOutputOperator.getName, round, path)
            .map { foo =>
              (foo.id.get, foo.foo.getAsString)
            }.collect.toSeq.sortBy(_._1)
          assert(result ===
            (0 until 100).filterNot(_ % 2 == 0).map(i => (100 * round + i, s"foo${100 * round + i}")))
        }
      }
    }
  }

  for {
    iterativeExtension <- Seq(
      new IterativeExtension(),
      new IterativeExtension("round"))
  } {
    val conf = s"${configuration}, IterativeExtension: ${iterativeExtension}"

    it should s"compile IterativeBatchExecutor with Checkpoint and Extract: [${conf}]" in { implicit sc =>
      val path = createTempDirectoryForEach("test-").toFile

      prepareData("foos", path) {
        sc.parallelize(0 until 100).map(Foo.intToFoo)
      }

      val inputOperator = ExternalInput
        .newInstance("foos/part-*",
          new ExternalInputInfo.Basic(
            ClassDescription.of(classOf[Foo]),
            "test",
            ClassDescription.of(classOf[Foo]),
            ExternalInputInfo.DataSize.UNKNOWN))

      val roundFoo = OperatorExtractor
        .extract(classOf[Update], classOf[Ops], "roundFoo")
        .input("foo", ClassDescription.of(classOf[Foo]), inputOperator.getOperatorPort)
        .output("output", ClassDescription.of(classOf[Foo]))
        .attribute(classOf[IterativeExtension], iterativeExtension)
        .build()

      val checkpointOperator = CoreOperator
        .builder(CoreOperator.CoreOperatorKind.CHECKPOINT)
        .input("input", ClassDescription.of(classOf[Foo]), roundFoo.findOutput("output"))
        .output("output", ClassDescription.of(classOf[Foo]))
        .build()

      val extractOperator = OperatorExtractor
        .extract(classOf[Extract], classOf[Ops], "extract")
        .input("foo", ClassDescription.of(classOf[Foo]), checkpointOperator.findOutput("output"))
        .output("evenResult", ClassDescription.of(classOf[Foo]))
        .output("oddResult", ClassDescription.of(classOf[Foo]))
        .build()

      val evenOutputOperator = ExternalOutput
        .newInstance("even", extractOperator.findOutput("evenResult"))

      val oddOutputOperator = ExternalOutput
        .newInstance("odd", extractOperator.findOutput("oddResult"))

      val graph = new OperatorGraph(Seq(
        inputOperator,
        checkpointOperator,
        extractOperator,
        evenOutputOperator,
        oddOutputOperator))

      val executorType = compile(flowId, graph, 4, path, classServer.root.toFile)

      val rounds = 0 to 1
      execute(flowId, executorType, rounds)

      for {
        round <- rounds
      } {
        {
          val result = readResult[Foo](evenOutputOperator.getName, round, path)
            .map { foo =>
              (foo.id.get, foo.foo.getAsString)
            }.collect.toSeq.sortBy(_._1)
          assert(result ===
            (0 until 100).filter(_ % 2 == 0).map(i => (100 * round + i, s"foo${100 * round + i}")))
        }
        {
          val result = readResult[Foo](oddOutputOperator.getName, round, path)
            .map { foo =>
              (foo.id.get, foo.foo.getAsString)
            }.collect.toSeq.sortBy(_._1)
          assert(result ===
            (0 until 100).filterNot(_ % 2 == 0).map(i => (100 * round + i, s"foo${100 * round + i}")))
        }
      }
    }
  }

  for {
    iterativeExtension <- Seq(
      new IterativeExtension(),
      new IterativeExtension("round"))
  } {
    val conf = s"${configuration}, IterativeExtension: ${iterativeExtension}"

    it should s"compile IterativeBatchExecutor with StopFragment: [${conf}]" in { implicit sc =>
      val path = createTempDirectoryForEach("test-").toFile

      prepareData("foos", path) {
        sc.parallelize(0 until 100).map(Foo.intToFoo)
      }

      val inputOperator = ExternalInput
        .newInstance("foos/part-*",
          new ExternalInputInfo.Basic(
            ClassDescription.of(classOf[Foo]),
            "test",
            ClassDescription.of(classOf[Foo]),
            ExternalInputInfo.DataSize.UNKNOWN))

      val roundFoo = OperatorExtractor
        .extract(classOf[Update], classOf[Ops], "roundFoo")
        .input("foo", ClassDescription.of(classOf[Foo]), inputOperator.getOperatorPort)
        .output("output", ClassDescription.of(classOf[Foo]))
        .attribute(classOf[IterativeExtension], iterativeExtension)
        .build()

      val extractOperator = OperatorExtractor
        .extract(classOf[Extract], classOf[Ops], "extract")
        .input("foo", ClassDescription.of(classOf[Foo]), roundFoo.findOutput("output"))
        .output("evenResult", ClassDescription.of(classOf[Foo]))
        .output("oddResult", ClassDescription.of(classOf[Foo]))
        .build()

      val evenOutputOperator = ExternalOutput
        .newInstance("even", extractOperator.findOutput("evenResult"))

      val graph = new OperatorGraph(Seq(
        inputOperator,
        extractOperator,
        evenOutputOperator))

      val executorType = compile(flowId, graph, 2, path, classServer.root.toFile)

      val rounds = 0 to 1
      execute(flowId, executorType, rounds)

      for {
        round <- rounds
      } {
        {
          val result = readResult[Foo](evenOutputOperator.getName, round, path)
            .map { foo =>
              (foo.id.get, foo.foo.getAsString)
            }.collect.toSeq.sortBy(_._1)
          assert(result ===
            (0 until 100).filter(_ % 2 == 0).map(i => (100 * round + i, s"foo${100 * round + i}")))
        }
      }
    }
  }

  for {
    iterativeExtension <- Seq(
      new IterativeExtension(),
      new IterativeExtension("round"))
  } {
    val conf = s"${configuration}, IterativeExtension: ${iterativeExtension}"

    it should s"compile IterativeBatchExecutor with CoGroup: [${conf}]" in { implicit sc =>
      val path = createTempDirectoryForEach("test-").toFile

      prepareData("foos1", path) {
        sc.parallelize(0 until 5).map(Foo.intToFoo)
      }
      prepareData("foos2", path) {
        sc.parallelize(5 until 10).map(Foo.intToFoo)
      }
      prepareData("bars", path) {
        sc.parallelize(0 until 10).flatMap(Bar.intToBars)
      }

      val foo1InputOperator = ExternalInput
        .newInstance("foos1/part-*",
          new ExternalInputInfo.Basic(
            ClassDescription.of(classOf[Foo]),
            "foos1",
            ClassDescription.of(classOf[Foo]),
            ExternalInputInfo.DataSize.UNKNOWN))

      val roundFoo1 = OperatorExtractor
        .extract(classOf[Update], classOf[Ops], "roundFoo")
        .input("foo", ClassDescription.of(classOf[Foo]), foo1InputOperator.getOperatorPort)
        .output("output", ClassDescription.of(classOf[Foo]))
        .attribute(classOf[IterativeExtension], iterativeExtension)
        .build()

      val foo2InputOperator = ExternalInput
        .newInstance("foos2/part-*",
          new ExternalInputInfo.Basic(
            ClassDescription.of(classOf[Foo]),
            "foos2",
            ClassDescription.of(classOf[Foo]),
            ExternalInputInfo.DataSize.UNKNOWN))

      val roundFoo2 = OperatorExtractor
        .extract(classOf[Update], classOf[Ops], "roundFoo")
        .input("foo", ClassDescription.of(classOf[Foo]), foo2InputOperator.getOperatorPort)
        .output("output", ClassDescription.of(classOf[Foo]))
        .attribute(classOf[IterativeExtension], iterativeExtension)
        .build()

      val barInputOperator = ExternalInput
        .newInstance("bars/part-*",
          new ExternalInputInfo.Basic(
            ClassDescription.of(classOf[Bar]),
            "bars",
            ClassDescription.of(classOf[Bar]),
            ExternalInputInfo.DataSize.UNKNOWN))

      val roundBar = OperatorExtractor
        .extract(classOf[Update], classOf[Ops], "roundBar")
        .input("bar", ClassDescription.of(classOf[Bar]), barInputOperator.getOperatorPort)
        .output("output", ClassDescription.of(classOf[Bar]))
        .attribute(classOf[IterativeExtension], iterativeExtension)
        .build()

      val cogroupOperator = OperatorExtractor
        .extract(classOf[CoGroup], classOf[Ops], "cogroup")
        .input("foos", ClassDescription.of(classOf[Foo]),
          Groups.parse(Seq("id")),
          roundFoo1.findOutput("output"), roundFoo2.findOutput("output"))
        .input("bars", ClassDescription.of(classOf[Bar]),
          Groups.parse(Seq("fooId"), Seq("+id")),
          roundBar.findOutput("output"))
        .output("fooResult", ClassDescription.of(classOf[Foo]))
        .output("barResult", ClassDescription.of(classOf[Bar]))
        .output("fooError", ClassDescription.of(classOf[Foo]))
        .output("barError", ClassDescription.of(classOf[Bar]))
        .build()

      val fooResultOutputOperator = ExternalOutput
        .newInstance("fooResult", cogroupOperator.findOutput("fooResult"))

      val barResultOutputOperator = ExternalOutput
        .newInstance("barResult", cogroupOperator.findOutput("barResult"))

      val fooErrorOutputOperator = ExternalOutput
        .newInstance("fooError", cogroupOperator.findOutput("fooError"))

      val barErrorOutputOperator = ExternalOutput
        .newInstance("barError", cogroupOperator.findOutput("barError"))

      val graph = new OperatorGraph(Seq(
        foo1InputOperator, foo2InputOperator, barInputOperator,
        cogroupOperator,
        fooResultOutputOperator, barResultOutputOperator, fooErrorOutputOperator, barErrorOutputOperator))

      val executorType = compile(flowId, graph, 8, path, classServer.root.toFile)

      val rounds = 0 to 1
      execute(flowId, executorType, rounds)

      for {
        round <- rounds
      } {
        {
          val fooResult = readResult[Foo](fooResultOutputOperator.getName, round, path)
            .map { foo =>
              (foo.id.get, foo.foo.getAsString)
            }.collect.toSeq.sortBy(_._1)
          assert(fooResult.size === 1)
          assert(fooResult(0) === (100 * round + 1, s"foo${100 * round + 1}"))
        }
        {
          val barResult = readResult[Bar](barResultOutputOperator.getName, round, path)
            .map { bar =>
              (bar.id.get, bar.fooId.get, bar.bar.getAsString)
            }.collect.toSeq.sortBy(_._1)
          assert(barResult.size === 1)
          assert(barResult(0) === (100 * round + 10, 100 * round + 1, s"bar${100 * round + 10}"))
        }
        {
          val fooError = readResult[Foo](fooErrorOutputOperator.getName, round, path)
            .map { foo =>
              (foo.id.get, foo.foo.getAsString)
            }.collect.toSeq.sortBy(_._1)
          assert(fooError.size === 9)
          assert(fooError(0) === (100 * round + 0, s"foo${100 * round + 0}"))
          for (i <- 2 until 10) {
            assert(fooError(i - 1) === (100 * round + i, s"foo${100 * round + i}"))
          }
        }
        {
          val barError = readResult[Bar](barErrorOutputOperator.getName, round, path)
            .map { bar =>
              (bar.id.get, bar.fooId.get, bar.bar.getAsString)
            }.collect.toSeq.sortBy(bar => (bar._2, bar._1))
          assert(barError.size === 44)
          for {
            i <- 2 until 10
            j <- 0 until i
          } {
            assert(barError((i * (i - 1)) / 2 + j - 1) ===
              (100 * round + 10 + j, 100 * round + i, s"bar${100 * round + 10 + j}"))
          }
        }
      }
    }
  }

  for {
    iterativeExtension <- Seq(
      new IterativeExtension(),
      new IterativeExtension("round"))
  } {
    val conf = s"${configuration}, IterativeExtension: ${iterativeExtension}"

    it should s"compile IterativeBatchExecutor with CoGroup with grouping is empty: [${conf}]" in { implicit sc =>
      val path = createTempDirectoryForEach("test-").toFile

      prepareData("foos1", path) {
        sc.parallelize(0 until 5).map(Foo.intToFoo)
      }
      prepareData("foos2", path) {
        sc.parallelize(5 until 10).map(Foo.intToFoo)
      }
      prepareData("bars", path) {
        sc.parallelize(0 until 10).flatMap(Bar.intToBars)
      }

      val foo1InputOperator = ExternalInput
        .newInstance("foos1/part-*",
          new ExternalInputInfo.Basic(
            ClassDescription.of(classOf[Foo]),
            "foos1",
            ClassDescription.of(classOf[Foo]),
            ExternalInputInfo.DataSize.UNKNOWN))

      val roundFoo1 = OperatorExtractor
        .extract(classOf[Update], classOf[Ops], "roundFoo")
        .input("foo", ClassDescription.of(classOf[Foo]), foo1InputOperator.getOperatorPort)
        .output("output", ClassDescription.of(classOf[Foo]))
        .attribute(classOf[IterativeExtension], iterativeExtension)
        .build()

      val foo2InputOperator = ExternalInput
        .newInstance("foos2/part-*",
          new ExternalInputInfo.Basic(
            ClassDescription.of(classOf[Foo]),
            "foos2",
            ClassDescription.of(classOf[Foo]),
            ExternalInputInfo.DataSize.UNKNOWN))

      val roundFoo2 = OperatorExtractor
        .extract(classOf[Update], classOf[Ops], "roundFoo")
        .input("foo", ClassDescription.of(classOf[Foo]), foo2InputOperator.getOperatorPort)
        .output("output", ClassDescription.of(classOf[Foo]))
        .attribute(classOf[IterativeExtension], iterativeExtension)
        .build()

      val barInputOperator = ExternalInput
        .newInstance("bars/part-*",
          new ExternalInputInfo.Basic(
            ClassDescription.of(classOf[Bar]),
            "bars",
            ClassDescription.of(classOf[Bar]),
            ExternalInputInfo.DataSize.UNKNOWN))

      val roundBar = OperatorExtractor
        .extract(classOf[Update], classOf[Ops], "roundBar")
        .input("bar", ClassDescription.of(classOf[Bar]), barInputOperator.getOperatorPort)
        .output("output", ClassDescription.of(classOf[Bar]))
        .attribute(classOf[IterativeExtension], iterativeExtension)
        .build()

      val cogroupOperator = OperatorExtractor
        .extract(classOf[CoGroup], classOf[Ops], "cogroup")
        .input("foos", ClassDescription.of(classOf[Foo]),
          Groups.parse(Seq.empty[String]),
          roundFoo1.findOutput("output"), roundFoo2.findOutput("output"))
        .input("bars", ClassDescription.of(classOf[Bar]),
          Groups.parse(Seq.empty[String], Seq("+id")),
          roundBar.findOutput("output"))
        .output("fooResult", ClassDescription.of(classOf[Foo]))
        .output("barResult", ClassDescription.of(classOf[Bar]))
        .output("fooError", ClassDescription.of(classOf[Foo]))
        .output("barError", ClassDescription.of(classOf[Bar]))
        .build()

      val fooResultOutputOperator = ExternalOutput
        .newInstance("fooResult", cogroupOperator.findOutput("fooResult"))

      val barResultOutputOperator = ExternalOutput
        .newInstance("barResult", cogroupOperator.findOutput("barResult"))

      val fooErrorOutputOperator = ExternalOutput
        .newInstance("fooError", cogroupOperator.findOutput("fooError"))

      val barErrorOutputOperator = ExternalOutput
        .newInstance("barError", cogroupOperator.findOutput("barError"))

      val graph = new OperatorGraph(Seq(
        foo1InputOperator, foo2InputOperator, barInputOperator,
        cogroupOperator,
        fooResultOutputOperator, barResultOutputOperator, fooErrorOutputOperator, barErrorOutputOperator))

      val executorType = compile(flowId, graph, 8, path, classServer.root.toFile)

      val rounds = 0 to 1
      execute(flowId, executorType, rounds)

      for {
        round <- rounds
      } {
        {
          val fooResult = readResult[Foo](fooResultOutputOperator.getName, round, path)
            .map { foo =>
              (foo.id.get, foo.foo.getAsString)
            }.collect.toSeq.sortBy(_._1)
          assert(fooResult.size === 0)
        }
        {
          val barResult = readResult[Bar](barResultOutputOperator.getName, round, path)
            .map { bar =>
              (bar.id.get, bar.fooId.get, bar.bar.getAsString)
            }.collect.toSeq.sortBy(_._1)
          assert(barResult.size === 0)
        }
        {
          val fooError = readResult[Foo](fooErrorOutputOperator.getName, round, path)
            .map { foo =>
              (foo.id.get, foo.foo.getAsString)
            }.collect.toSeq.sortBy(_._1)
          assert(fooError.size === 10)
          for (i <- 0 until 10) {
            assert(fooError(i) === (100 * round + i, s"foo${100 * round + i}"))
          }
        }
        {
          val barError = readResult[Bar](barErrorOutputOperator.getName, round, path)
            .map { bar =>
              (bar.id.get, bar.fooId.get, bar.bar.getAsString)
            }.collect.toSeq.sortBy(bar => (bar._2, bar._1))
          assert(barError.size === 45)
          for {
            i <- 0 until 10
            j <- 0 until i
          } {
            assert(barError((i * (i - 1)) / 2 + j) ===
              (100 * round + 10 + j, 100 * round + i, s"bar${100 * round + 10 + j}"))
          }
        }
      }
    }
  }

  for {
    iterativeExtension <- Seq(
      new IterativeExtension(),
      new IterativeExtension("round"))
  } {
    val conf = s"${configuration}, IterativeExtension: ${iterativeExtension}"

    it should s"compile IterativeBatchExecutor with MasterCheck: [${conf}]" in { implicit sc =>
      val path = createTempDirectoryForEach("test-").toFile

      prepareData("foos", path) {
        sc.parallelize(0 until 10).map(Foo.intToFoo)
      }
      prepareData("bars", path) {
        sc.parallelize(5 until 15).map(Bar.intToBar)
      }

      val fooInputOperator = ExternalInput
        .newInstance("foos/part-*",
          new ExternalInputInfo.Basic(
            ClassDescription.of(classOf[Foo]),
            "foos",
            ClassDescription.of(classOf[Foo]),
            ExternalInputInfo.DataSize.UNKNOWN))

      val roundFoo1 = OperatorExtractor
        .extract(classOf[Update], classOf[Ops], "roundFoo")
        .input("foo", ClassDescription.of(classOf[Foo]), fooInputOperator.getOperatorPort)
        .output("output", ClassDescription.of(classOf[Foo]))
        .attribute(classOf[IterativeExtension], iterativeExtension)
        .build()

      val barInputOperator = ExternalInput
        .newInstance("bars/part-*",
          new ExternalInputInfo.Basic(
            ClassDescription.of(classOf[Bar]),
            "bars",
            ClassDescription.of(classOf[Bar]),
            ExternalInputInfo.DataSize.UNKNOWN))

      val roundBar = OperatorExtractor
        .extract(classOf[Update], classOf[Ops], "roundBar")
        .input("bar", ClassDescription.of(classOf[Bar]), barInputOperator.getOperatorPort)
        .output("output", ClassDescription.of(classOf[Bar]))
        .attribute(classOf[IterativeExtension], iterativeExtension)
        .build()

      val masterCheckOperator = OperatorExtractor
        .extract(classOf[MasterCheck], classOf[Ops], "mastercheck")
        .input("foos", ClassDescription.of(classOf[Foo]),
          Groups.parse(Seq("id")),
          roundFoo1.findOutput("output"))
        .input("bars", ClassDescription.of(classOf[Bar]),
          Groups.parse(Seq("fooId"), Seq("+id")),
          roundBar.findOutput("output"))
        .output("found", ClassDescription.of(classOf[Bar]))
        .output("missed", ClassDescription.of(classOf[Bar]))
        .build()

      val foundOutputOperator = ExternalOutput
        .newInstance("found", masterCheckOperator.findOutput("found"))

      val missedOutputOperator = ExternalOutput
        .newInstance("missed", masterCheckOperator.findOutput("missed"))

      val graph = new OperatorGraph(Seq(
        fooInputOperator, barInputOperator,
        masterCheckOperator,
        foundOutputOperator, missedOutputOperator))

      val executorType = compile(flowId, graph, 5, path, classServer.root.toFile)

      val rounds = 0 to 1
      execute(flowId, executorType, rounds)

      for {
        round <- rounds
      } {
        {
          val found = readResult[Bar](foundOutputOperator.getName, round, path)
            .map { bar =>
              (bar.id.get, bar.fooId.get, bar.bar.getAsString)
            }.collect.toSeq.sortBy(_._1)
          assert(found.size === 5)
          assert(found ===
            (5 until 10).map(i => (100 * round + 10 + i, 100 * round + i, s"bar${100 * round + 10 + i}")))
        }
        {
          val missed = readResult[Bar](missedOutputOperator.getName, round, path)
            .map { bar =>
              (bar.id.get, bar.fooId.get, bar.bar.getAsString)
            }.collect.toSeq.sortBy(_._1)
          assert(missed.size === 5)
          assert(missed ===
            (10 until 15).map(i => (100 * round + 10 + i, 100 * round + i, s"bar${100 * round + 10 + i}")))
        }
      }
    }

    it should s"compile IterativeBatchExecutor with MasterCheck with multiple masters: [${conf}]" in { implicit sc =>
      val path = createTempDirectoryForEach("test-").toFile

      prepareData("foos1", path) {
        sc.parallelize(0 until 5).map(Foo.intToFoo)
      }
      prepareData("foos2", path) {
        sc.parallelize(5 until 10).map(Foo.intToFoo)
      }
      prepareData("bars", path) {
        sc.parallelize(5 until 15).map(Bar.intToBar)
      }

      val foo1InputOperator = ExternalInput
        .newInstance("foos1/part-*",
          new ExternalInputInfo.Basic(
            ClassDescription.of(classOf[Foo]),
            "foos1",
            ClassDescription.of(classOf[Foo]),
            ExternalInputInfo.DataSize.UNKNOWN))

      val roundFoo1 = OperatorExtractor
        .extract(classOf[Update], classOf[Ops], "roundFoo")
        .input("foo", ClassDescription.of(classOf[Foo]), foo1InputOperator.getOperatorPort)
        .output("output", ClassDescription.of(classOf[Foo]))
        .attribute(classOf[IterativeExtension], iterativeExtension)
        .build()

      val foo2InputOperator = ExternalInput
        .newInstance("foos2/part-*",
          new ExternalInputInfo.Basic(
            ClassDescription.of(classOf[Foo]),
            "foos2",
            ClassDescription.of(classOf[Foo]),
            ExternalInputInfo.DataSize.UNKNOWN))

      val roundFoo2 = OperatorExtractor
        .extract(classOf[Update], classOf[Ops], "roundFoo")
        .input("foo", ClassDescription.of(classOf[Foo]), foo2InputOperator.getOperatorPort)
        .output("output", ClassDescription.of(classOf[Foo]))
        .attribute(classOf[IterativeExtension], iterativeExtension)
        .build()

      val barInputOperator = ExternalInput
        .newInstance("bars/part-*",
          new ExternalInputInfo.Basic(
            ClassDescription.of(classOf[Bar]),
            "bars",
            ClassDescription.of(classOf[Bar]),
            ExternalInputInfo.DataSize.UNKNOWN))

      val roundBar = OperatorExtractor
        .extract(classOf[Update], classOf[Ops], "roundBar")
        .input("bar", ClassDescription.of(classOf[Bar]), barInputOperator.getOperatorPort)
        .output("output", ClassDescription.of(classOf[Bar]))
        .attribute(classOf[IterativeExtension], iterativeExtension)
        .build()

      val masterCheckOperator = OperatorExtractor
        .extract(classOf[MasterCheck], classOf[Ops], "mastercheck")
        .input("foos", ClassDescription.of(classOf[Foo]),
          Groups.parse(Seq("id")),
          roundFoo1.findOutput("output"), roundFoo2.findOutput("output"))
        .input("bars", ClassDescription.of(classOf[Bar]),
          Groups.parse(Seq("fooId"), Seq("+id")),
          roundBar.findOutput("output"))
        .output("found", ClassDescription.of(classOf[Bar]))
        .output("missed", ClassDescription.of(classOf[Bar]))
        .build()

      val foundOutputOperator = ExternalOutput
        .newInstance("found", masterCheckOperator.findOutput("found"))

      val missedOutputOperator = ExternalOutput
        .newInstance("missed", masterCheckOperator.findOutput("missed"))

      val graph = new OperatorGraph(Seq(
        foo1InputOperator, foo2InputOperator, barInputOperator,
        masterCheckOperator,
        foundOutputOperator, missedOutputOperator))

      val executorType = compile(flowId, graph, 6, path, classServer.root.toFile)

      val rounds = 0 to 1
      execute(flowId, executorType, rounds)

      for {
        round <- rounds
      } {
        {
          val found = readResult[Bar](foundOutputOperator.getName, round, path)
            .map { bar =>
              (bar.id.get, bar.fooId.get, bar.bar.getAsString)
            }.collect.toSeq.sortBy(_._1)
          assert(found.size === 5)
          assert(found ===
            (5 until 10).map(i => (100 * round + 10 + i, 100 * round + i, s"bar${100 * round + 10 + i}")))
        }
        {
          val missed = readResult[Bar](missedOutputOperator.getName, round, path)
            .map { bar =>
              (bar.id.get, bar.fooId.get, bar.bar.getAsString)
            }.collect.toSeq.sortBy(_._1)
          assert(missed.size === 5)
          assert(missed ===
            (10 until 15).map(i => (100 * round + 10 + i, 100 * round + i, s"bar${100 * round + 10 + i}")))
        }
      }
    }

    it should s"compile IterativeBatchExecutor with MasterCheck with fixed master: [${conf}]" in { implicit sc =>
      val path = createTempDirectoryForEach("test-").toFile
      val rounds = 0 to 1

      prepareData("foos1", path) {
        sc.parallelize(0 until 5).map(Foo.intToFoo).flatMap { foo =>
          rounds.iterator.map { round =>
            Foo.round(foo, round)
          }
        }
      }
      prepareData("foos2", path) {
        sc.parallelize(5 until 10).map(Foo.intToFoo).flatMap { foo =>
          rounds.iterator.map { round =>
            Foo.round(foo, round)
          }
        }
      }
      prepareData("bars", path) {
        sc.parallelize(5 until 15).map(Bar.intToBar)
      }

      val foo1InputOperator = ExternalInput
        .newInstance("foos1/part-*",
          new ExternalInputInfo.Basic(
            ClassDescription.of(classOf[Foo]),
            "foos1",
            ClassDescription.of(classOf[Foo]),
            ExternalInputInfo.DataSize.UNKNOWN))

      val foo2InputOperator = ExternalInput
        .newInstance("foos2/part-*",
          new ExternalInputInfo.Basic(
            ClassDescription.of(classOf[Foo]),
            "foos2",
            ClassDescription.of(classOf[Foo]),
            ExternalInputInfo.DataSize.UNKNOWN))

      val barInputOperator = ExternalInput
        .newInstance("bars/part-*",
          new ExternalInputInfo.Basic(
            ClassDescription.of(classOf[Bar]),
            "bars",
            ClassDescription.of(classOf[Bar]),
            ExternalInputInfo.DataSize.UNKNOWN))

      val roundBar = OperatorExtractor
        .extract(classOf[Update], classOf[Ops], "roundBar")
        .input("bar", ClassDescription.of(classOf[Bar]), barInputOperator.getOperatorPort)
        .output("output", ClassDescription.of(classOf[Bar]))
        .attribute(classOf[IterativeExtension], iterativeExtension)
        .build()

      val masterCheckOperator = OperatorExtractor
        .extract(classOf[MasterCheck], classOf[Ops], "mastercheck")
        .input("foos", ClassDescription.of(classOf[Foo]),
          Groups.parse(Seq("id")),
          foo1InputOperator.getOperatorPort, foo2InputOperator.getOperatorPort)
        .input("bars", ClassDescription.of(classOf[Bar]),
          Groups.parse(Seq("fooId"), Seq("+id")),
          roundBar.findOutput("output"))
        .output("found", ClassDescription.of(classOf[Bar]))
        .output("missed", ClassDescription.of(classOf[Bar]))
        .build()

      val foundOutputOperator = ExternalOutput
        .newInstance("found", masterCheckOperator.findOutput("found"))

      val missedOutputOperator = ExternalOutput
        .newInstance("missed", masterCheckOperator.findOutput("missed"))

      val graph = new OperatorGraph(Seq(
        foo1InputOperator, foo2InputOperator, barInputOperator,
        masterCheckOperator,
        foundOutputOperator, missedOutputOperator))

      val executorType = compile(flowId, graph, 6, path, classServer.root.toFile)
      execute(flowId, executorType, rounds)

      for {
        round <- rounds
      } {
        {
          val found = readResult[Bar](foundOutputOperator.getName, round, path)
            .map { bar =>
              (bar.id.get, bar.fooId.get, bar.bar.getAsString)
            }.collect.toSeq.sortBy(_._1)
          assert(found.size === 5)
          assert(found ===
            (5 until 10).map(i => (100 * round + 10 + i, 100 * round + i, s"bar${100 * round + 10 + i}")))
        }
        {
          val missed = readResult[Bar](missedOutputOperator.getName, round, path)
            .map { bar =>
              (bar.id.get, bar.fooId.get, bar.bar.getAsString)
            }.collect.toSeq.sortBy(_._1)
          assert(missed.size === 5)
          assert(missed ===
            (10 until 15).map(i => (100 * round + 10 + i, 100 * round + i, s"bar${100 * round + 10 + i}")))
        }
      }
    }
  }

  for {
    iterativeExtension <- Seq(
      new IterativeExtension(),
      new IterativeExtension("round"))
  } {
    val conf = s"${configuration}, IterativeExtension: ${iterativeExtension}"

    it should s"compile IterativeBatchExecutor with broadcast MasterCheck: [${conf}]" in { implicit sc =>
      val path = createTempDirectoryForEach("test-").toFile

      prepareData("foos", path) {
        sc.parallelize(0 until 10).map(Foo.intToFoo)
      }
      prepareData("bars", path) {
        sc.parallelize(5 until 15).map(Bar.intToBar)
      }

      val fooInputOperator = ExternalInput
        .newInstance("foos/part-*",
          new ExternalInputInfo.Basic(
            ClassDescription.of(classOf[Foo]),
            "foos",
            ClassDescription.of(classOf[Foo]),
            ExternalInputInfo.DataSize.TINY))

      val roundFoo = OperatorExtractor
        .extract(classOf[Update], classOf[Ops], "roundFoo")
        .input("foo", ClassDescription.of(classOf[Foo]), fooInputOperator.getOperatorPort)
        .output("output", ClassDescription.of(classOf[Foo]))
        .attribute(classOf[IterativeExtension], iterativeExtension)
        .build()

      val barInputOperator = ExternalInput
        .newInstance("bars/part-*",
          new ExternalInputInfo.Basic(
            ClassDescription.of(classOf[Bar]),
            "bars",
            ClassDescription.of(classOf[Bar]),
            ExternalInputInfo.DataSize.UNKNOWN))

      val roundBar = OperatorExtractor
        .extract(classOf[Update], classOf[Ops], "roundBar")
        .input("bar", ClassDescription.of(classOf[Bar]), barInputOperator.getOperatorPort)
        .output("output", ClassDescription.of(classOf[Bar]))
        .attribute(classOf[IterativeExtension], iterativeExtension)
        .build()

      val masterCheckOperator = OperatorExtractor
        .extract(classOf[MasterCheck], classOf[Ops], "mastercheck")
        .input("foos", ClassDescription.of(classOf[Foo]),
          Groups.parse(Seq("id")),
          roundFoo.findOutput("output"))
        .input("bars", ClassDescription.of(classOf[Bar]),
          Groups.parse(Seq("fooId"), Seq("+id")),
          roundBar.findOutput("output"))
        .output("found", ClassDescription.of(classOf[Bar]))
        .output("missed", ClassDescription.of(classOf[Bar]))
        .build()

      val foundOutputOperator = ExternalOutput
        .newInstance("found", masterCheckOperator.findOutput("found"))

      val missedOutputOperator = ExternalOutput
        .newInstance("missed", masterCheckOperator.findOutput("missed"))

      val graph = new OperatorGraph(Seq(
        fooInputOperator, barInputOperator,
        masterCheckOperator,
        foundOutputOperator, missedOutputOperator))

      val executorType = compile(
        flowId, graph, 4, path, classServer.root.toFile,
        Map("operator.estimator.Update" -> "1.0"))

      val rounds = 0 to 1
      execute(flowId, executorType, rounds)

      for {
        round <- rounds
      } {
        {
          val found = readResult[Bar](foundOutputOperator.getName, round, path)
            .map { bar =>
              (bar.id.get, bar.fooId.get, bar.bar.getAsString)
            }.collect.toSeq.sortBy(_._1)
          assert(found.size === 5)
          assert(found ===
            (5 until 10).map(i => (100 * round + 10 + i, 100 * round + i, s"bar${100 * round + 10 + i}")))
        }
        {
          val missed = readResult[Bar](missedOutputOperator.getName, round, path)
            .map { bar =>
              (bar.id.get, bar.fooId.get, bar.bar.getAsString)
            }.collect.toSeq.sortBy(_._1)
          assert(missed.size === 5)
          assert(missed ===
            (10 until 15).map(i => (100 * round + 10 + i, 100 * round + i, s"bar${100 * round + 10 + i}")))
        }
      }
    }

    it should s"compile IterativeBatchExecutor with broadcast MasterCheck with multiple masters: [${conf}]" in { implicit sc =>
      val path = createTempDirectoryForEach("test-").toFile

      prepareData("foos1", path) {
        sc.parallelize(0 until 5).map(Foo.intToFoo)
      }
      prepareData("foos2", path) {
        sc.parallelize(5 until 10).map(Foo.intToFoo)
      }
      prepareData("bars", path) {
        sc.parallelize(5 until 15).map(Bar.intToBar)
      }

      val foo1InputOperator = ExternalInput
        .newInstance("foos1/part-*",
          new ExternalInputInfo.Basic(
            ClassDescription.of(classOf[Foo]),
            "foos1",
            ClassDescription.of(classOf[Foo]),
            ExternalInputInfo.DataSize.TINY))

      val roundFoo1 = OperatorExtractor
        .extract(classOf[Update], classOf[Ops], "roundFoo")
        .input("foo1", ClassDescription.of(classOf[Foo]), foo1InputOperator.getOperatorPort)
        .output("output", ClassDescription.of(classOf[Foo]))
        .attribute(classOf[IterativeExtension], iterativeExtension)
        .build()

      val foo2InputOperator = ExternalInput
        .newInstance("foos2/part-*",
          new ExternalInputInfo.Basic(
            ClassDescription.of(classOf[Foo]),
            "foos2",
            ClassDescription.of(classOf[Foo]),
            ExternalInputInfo.DataSize.TINY))

      val roundFoo2 = OperatorExtractor
        .extract(classOf[Update], classOf[Ops], "roundFoo")
        .input("foo2", ClassDescription.of(classOf[Foo]), foo2InputOperator.getOperatorPort)
        .output("output", ClassDescription.of(classOf[Foo]))
        .attribute(classOf[IterativeExtension], iterativeExtension)
        .build()

      val barInputOperator = ExternalInput
        .newInstance("bars/part-*",
          new ExternalInputInfo.Basic(
            ClassDescription.of(classOf[Bar]),
            "bars",
            ClassDescription.of(classOf[Bar]),
            ExternalInputInfo.DataSize.UNKNOWN))

      val roundBar = OperatorExtractor
        .extract(classOf[Update], classOf[Ops], "roundBar")
        .input("bar", ClassDescription.of(classOf[Bar]), barInputOperator.getOperatorPort)
        .output("output", ClassDescription.of(classOf[Bar]))
        .attribute(classOf[IterativeExtension], iterativeExtension)
        .build()

      val masterCheckOperator = OperatorExtractor
        .extract(classOf[MasterCheck], classOf[Ops], "mastercheck")
        .input("foos", ClassDescription.of(classOf[Foo]),
          Groups.parse(Seq("id")),
          roundFoo1.findOutput("output"), roundFoo2.findOutput("output"))
        .input("bars", ClassDescription.of(classOf[Bar]),
          Groups.parse(Seq("fooId"), Seq("+id")),
          roundBar.findOutput("output"))
        .output("found", ClassDescription.of(classOf[Bar]))
        .output("missed", ClassDescription.of(classOf[Bar]))
        .build()

      val foundOutputOperator = ExternalOutput
        .newInstance("found", masterCheckOperator.findOutput("found"))

      val missedOutputOperator = ExternalOutput
        .newInstance("missed", masterCheckOperator.findOutput("missed"))

      val graph = new OperatorGraph(Seq(
        foo1InputOperator, foo2InputOperator, barInputOperator,
        masterCheckOperator,
        foundOutputOperator, missedOutputOperator))

      val executorType = compile(
        flowId, graph, 5, path, classServer.root.toFile,
        Map("operator.estimator.Update" -> "1.0"))

      val rounds = 0 to 1
      execute(flowId, executorType, rounds)

      for {
        round <- rounds
      } {
        {
          val found = readResult[Bar](foundOutputOperator.getName, round, path)
            .map { bar =>
              (bar.id.get, bar.fooId.get, bar.bar.getAsString)
            }.collect.toSeq.sortBy(_._1)
          assert(found.size === 5)
          assert(found ===
            (5 until 10).map(i => (100 * round + 10 + i, 100 * round + i, s"bar${100 * round + 10 + i}")))
        }
        {
          val missed = readResult[Bar](missedOutputOperator.getName, round, path)
            .map { bar =>
              (bar.id.get, bar.fooId.get, bar.bar.getAsString)
            }.collect.toSeq.sortBy(_._1)
          assert(missed.size === 5)
          assert(missed ===
            (10 until 15).map(i => (100 * round + 10 + i, 100 * round + i, s"bar${100 * round + 10 + i}")))
        }
      }
    }

    it should s"compile IterativeBatchExecutor with Checkpoint and broadcast MasterCheck: [${conf}]" in { implicit sc =>
      val path = createTempDirectoryForEach("test-").toFile

      prepareData("foos", path) {
        sc.parallelize(0 until 10).map(Foo.intToFoo)
      }
      prepareData("bars", path) {
        sc.parallelize(5 until 15).map(Bar.intToBar)
      }

      val fooInputOperator = ExternalInput
        .newInstance("foos/part-*",
          new ExternalInputInfo.Basic(
            ClassDescription.of(classOf[Foo]),
            "foos",
            ClassDescription.of(classOf[Foo]),
            ExternalInputInfo.DataSize.TINY))

      val roundFoo = OperatorExtractor
        .extract(classOf[Update], classOf[Ops], "roundFoo")
        .input("foo", ClassDescription.of(classOf[Foo]), fooInputOperator.getOperatorPort)
        .output("output", ClassDescription.of(classOf[Foo]))
        .attribute(classOf[IterativeExtension], iterativeExtension)
        .build()

      val barInputOperator = ExternalInput
        .newInstance("bars/part-*",
          new ExternalInputInfo.Basic(
            ClassDescription.of(classOf[Bar]),
            "bars",
            ClassDescription.of(classOf[Bar]),
            ExternalInputInfo.DataSize.UNKNOWN))

      val roundBar = OperatorExtractor
        .extract(classOf[Update], classOf[Ops], "roundBar")
        .input("bar", ClassDescription.of(classOf[Bar]), barInputOperator.getOperatorPort)
        .output("output", ClassDescription.of(classOf[Bar]))
        .attribute(classOf[IterativeExtension], iterativeExtension)
        .build()

      val checkpointOperator = CoreOperator
        .builder(CoreOperator.CoreOperatorKind.CHECKPOINT)
        .input("input", ClassDescription.of(classOf[Bar]), roundBar.findOutput("output"))
        .output("output", ClassDescription.of(classOf[Bar]))
        .build()

      val masterCheckOperator = OperatorExtractor
        .extract(classOf[MasterCheck], classOf[Ops], "mastercheck")
        .input("foos", ClassDescription.of(classOf[Foo]),
          Groups.parse(Seq("id")),
          roundFoo.findOutput("output"))
        .input("bars", ClassDescription.of(classOf[Bar]),
          Groups.parse(Seq("fooId"), Seq("+id")),
          checkpointOperator.findOutput("output"))
        .output("found", ClassDescription.of(classOf[Bar]))
        .output("missed", ClassDescription.of(classOf[Bar]))
        .build()

      val foundOutputOperator = ExternalOutput
        .newInstance("found", masterCheckOperator.findOutput("found"))

      val missedOutputOperator = ExternalOutput
        .newInstance("missed", masterCheckOperator.findOutput("missed"))

      val graph = new OperatorGraph(Seq(
        fooInputOperator, barInputOperator,
        masterCheckOperator,
        foundOutputOperator, missedOutputOperator))

      val executorType = compile(flowId, graph, 5, path, classServer.root.toFile)

      val rounds = 0 to 1
      execute(flowId, executorType, rounds)

      for {
        round <- rounds
      } {
        {
          val found = readResult[Bar](foundOutputOperator.getName, round, path)
            .map { bar =>
              (bar.id.get, bar.fooId.get, bar.bar.getAsString)
            }.collect.toSeq.sortBy(_._1)
          assert(found.size === 5)
          assert(found ===
            (5 until 10).map(i => (100 * round + 10 + i, 100 * round + i, s"bar${100 * round + 10 + i}")))
        }
        {
          val missed = readResult[Bar](missedOutputOperator.getName, round, path)
            .map { bar =>
              (bar.id.get, bar.fooId.get, bar.bar.getAsString)
            }.collect.toSeq.sortBy(_._1)
          assert(missed.size === 5)
          assert(missed ===
            (10 until 15).map(i => (100 * round + 10 + i, 100 * round + i, s"bar${100 * round + 10 + i}")))
        }
      }
    }

    it should s"compile IterativeBatchExecutor with broadcast MasterCheck with fixed master: [${conf}]" in { implicit sc =>
      val path = createTempDirectoryForEach("test-").toFile
      val rounds = 0 to 1

      prepareData("foos", path) {
        sc.parallelize(0 until 10).map(Foo.intToFoo).flatMap { foo =>
          rounds.iterator.map { round =>
            Foo.round(foo, round)
          }
        }
      }
      prepareData("bars", path) {
        sc.parallelize(5 until 15).map(Bar.intToBar)
      }

      val fooInputOperator = ExternalInput
        .newInstance("foos/part-*",
          new ExternalInputInfo.Basic(
            ClassDescription.of(classOf[Foo]),
            "foos",
            ClassDescription.of(classOf[Foo]),
            ExternalInputInfo.DataSize.TINY))

      val barInputOperator = ExternalInput
        .newInstance("bars/part-*",
          new ExternalInputInfo.Basic(
            ClassDescription.of(classOf[Bar]),
            "bars",
            ClassDescription.of(classOf[Bar]),
            ExternalInputInfo.DataSize.UNKNOWN))

      val roundBar = OperatorExtractor
        .extract(classOf[Update], classOf[Ops], "roundBar")
        .input("bar", ClassDescription.of(classOf[Bar]), barInputOperator.getOperatorPort)
        .output("output", ClassDescription.of(classOf[Bar]))
        .attribute(classOf[IterativeExtension], iterativeExtension)
        .build()

      val masterCheckOperator = OperatorExtractor
        .extract(classOf[MasterCheck], classOf[Ops], "mastercheck")
        .input("foos", ClassDescription.of(classOf[Foo]),
          Groups.parse(Seq("id")),
          fooInputOperator.getOperatorPort)
        .input("bars", ClassDescription.of(classOf[Bar]),
          Groups.parse(Seq("fooId"), Seq("+id")),
          roundBar.findOutput("output"))
        .output("found", ClassDescription.of(classOf[Bar]))
        .output("missed", ClassDescription.of(classOf[Bar]))
        .build()

      val foundOutputOperator = ExternalOutput
        .newInstance("found", masterCheckOperator.findOutput("found"))

      val missedOutputOperator = ExternalOutput
        .newInstance("missed", masterCheckOperator.findOutput("missed"))

      val graph = new OperatorGraph(Seq(
        fooInputOperator, barInputOperator,
        masterCheckOperator,
        foundOutputOperator, missedOutputOperator))

      val executorType = compile(flowId, graph, 4, path, classServer.root.toFile)
      execute(flowId, executorType, rounds)

      for {
        round <- rounds
      } {
        {
          val found = readResult[Bar](foundOutputOperator.getName, round, path)
            .map { bar =>
              (bar.id.get, bar.fooId.get, bar.bar.getAsString)
            }.collect.toSeq.sortBy(_._1)
          assert(found.size === 5)
          assert(found ===
            (5 until 10).map(i => (100 * round + 10 + i, 100 * round + i, s"bar${100 * round + 10 + i}")))
        }
        {
          val missed = readResult[Bar](missedOutputOperator.getName, round, path)
            .map { bar =>
              (bar.id.get, bar.fooId.get, bar.bar.getAsString)
            }.collect.toSeq.sortBy(_._1)
          assert(missed.size === 5)
          assert(missed ===
            (10 until 15).map(i => (100 * round + 10 + i, 100 * round + i, s"bar${100 * round + 10 + i}")))
        }
      }
    }
  }

  for {
    iterativeExtension <- Seq(
      new IterativeExtension(),
      new IterativeExtension("round"))
  } {
    val conf = s"${configuration}, IterativeExtension: ${iterativeExtension}"

    it should s"compile IterativeBatchExecutor with MasterJoin: [${conf}]" in { implicit sc =>
      val path = createTempDirectoryForEach("test-").toFile

      prepareData("foos1", path) {
        sc.parallelize(0 until 5).map(Foo.intToFoo)
      }
      prepareData("foos2", path) {
        sc.parallelize(5 until 10).map(Foo.intToFoo)
      }
      prepareData("bars", path) {
        sc.parallelize(5 until 15).map(Bar.intToBar)
      }

      val foo1InputOperator = ExternalInput
        .newInstance("foos1/part-*",
          new ExternalInputInfo.Basic(
            ClassDescription.of(classOf[Foo]),
            "foos1",
            ClassDescription.of(classOf[Foo]),
            ExternalInputInfo.DataSize.UNKNOWN))

      val roundFoo1 = OperatorExtractor
        .extract(classOf[Update], classOf[Ops], "roundFoo")
        .input("foo", ClassDescription.of(classOf[Foo]), foo1InputOperator.getOperatorPort)
        .output("output", ClassDescription.of(classOf[Foo]))
        .attribute(classOf[IterativeExtension], iterativeExtension)
        .build()

      val foo2InputOperator = ExternalInput
        .newInstance("foos2/part-*",
          new ExternalInputInfo.Basic(
            ClassDescription.of(classOf[Foo]),
            "foos2",
            ClassDescription.of(classOf[Foo]),
            ExternalInputInfo.DataSize.UNKNOWN))

      val roundFoo2 = OperatorExtractor
        .extract(classOf[Update], classOf[Ops], "roundFoo")
        .input("foo", ClassDescription.of(classOf[Foo]), foo2InputOperator.getOperatorPort)
        .output("output", ClassDescription.of(classOf[Foo]))
        .attribute(classOf[IterativeExtension], iterativeExtension)
        .build()

      val barInputOperator = ExternalInput
        .newInstance("bars/part-*",
          new ExternalInputInfo.Basic(
            ClassDescription.of(classOf[Bar]),
            "bars",
            ClassDescription.of(classOf[Bar]),
            ExternalInputInfo.DataSize.UNKNOWN))

      val roundBar = OperatorExtractor
        .extract(classOf[Update], classOf[Ops], "roundBar")
        .input("bar", ClassDescription.of(classOf[Bar]), barInputOperator.getOperatorPort)
        .output("output", ClassDescription.of(classOf[Bar]))
        .attribute(classOf[IterativeExtension], iterativeExtension)
        .build()

      val masterJoinOperator = OperatorExtractor
        .extract(classOf[MasterJoin], classOf[Ops], "masterjoin")
        .input("foos", ClassDescription.of(classOf[Foo]),
          Groups.parse(Seq("id")),
          roundFoo1.findOutput("output"), roundFoo2.findOutput("output"))
        .input("bars", ClassDescription.of(classOf[Bar]),
          Groups.parse(Seq("fooId"), Seq("+id")),
          roundBar.findOutput("output"))
        .output("joined", ClassDescription.of(classOf[FooBar]))
        .output("missed", ClassDescription.of(classOf[Bar]))
        .build()

      val joinedOutputOperator = ExternalOutput
        .newInstance("joined", masterJoinOperator.findOutput("joined"))

      val missedOutputOperator = ExternalOutput
        .newInstance("missed", masterJoinOperator.findOutput("missed"))

      val graph = new OperatorGraph(Seq(
        foo1InputOperator, foo2InputOperator, barInputOperator,
        masterJoinOperator,
        joinedOutputOperator, missedOutputOperator))

      val executorType = compile(flowId, graph, 6, path, classServer.root.toFile)

      val rounds = 0 to 1
      execute(flowId, executorType, rounds)

      for {
        round <- rounds
      } {
        {
          val found = readResult[FooBar](joinedOutputOperator.getName, round, path)
            .map { foobar =>
              (foobar.id.get, foobar.foo.getAsString, foobar.bar.getAsString)
            }.collect.toSeq.sortBy(_._1)
          assert(found.size === 5)
          assert(found ===
            (5 until 10).map(i => (100 * round + i, s"foo${100 * round + i}", s"bar${100 * round + 10 + i}")))
        }
        {
          val missed = readResult[Bar](missedOutputOperator.getName, round, path)
            .map { bar =>
              (bar.id.get, bar.fooId.get, bar.bar.getAsString)
            }.collect.toSeq.sortBy(_._1)
          assert(missed.size === 5)
          assert(missed ===
            (10 until 15).map(i => (100 * round + 10 + i, 100 * round + i, s"bar${100 * round + 10 + i}")))
        }
      }
    }

    it should s"compile IterativeBatchExecutor with MasterJoin with fixed master: [${conf}]" in { implicit sc =>
      val path = createTempDirectoryForEach("test-").toFile
      val rounds = 0 to 1

      prepareData("foos1", path) {
        sc.parallelize(0 until 5).map(Foo.intToFoo).flatMap { foo =>
          rounds.iterator.map { round =>
            Foo.round(foo, round)
          }
        }
      }
      prepareData("foos2", path) {
        sc.parallelize(5 until 10).map(Foo.intToFoo).flatMap { foo =>
          rounds.iterator.map { round =>
            Foo.round(foo, round)
          }
        }
      }
      prepareData("bars", path) {
        sc.parallelize(5 until 15).map(Bar.intToBar)
      }

      val foo1InputOperator = ExternalInput
        .newInstance("foos1/part-*",
          new ExternalInputInfo.Basic(
            ClassDescription.of(classOf[Foo]),
            "foos1",
            ClassDescription.of(classOf[Foo]),
            ExternalInputInfo.DataSize.UNKNOWN))

      val foo2InputOperator = ExternalInput
        .newInstance("foos2/part-*",
          new ExternalInputInfo.Basic(
            ClassDescription.of(classOf[Foo]),
            "foos2",
            ClassDescription.of(classOf[Foo]),
            ExternalInputInfo.DataSize.UNKNOWN))

      val barInputOperator = ExternalInput
        .newInstance("bars/part-*",
          new ExternalInputInfo.Basic(
            ClassDescription.of(classOf[Bar]),
            "bars",
            ClassDescription.of(classOf[Bar]),
            ExternalInputInfo.DataSize.UNKNOWN))

      val roundBar = OperatorExtractor
        .extract(classOf[Update], classOf[Ops], "roundBar")
        .input("bar", ClassDescription.of(classOf[Bar]), barInputOperator.getOperatorPort)
        .output("output", ClassDescription.of(classOf[Bar]))
        .attribute(classOf[IterativeExtension], iterativeExtension)
        .build()

      val masterJoinOperator = OperatorExtractor
        .extract(classOf[MasterJoin], classOf[Ops], "masterjoin")
        .input("foos", ClassDescription.of(classOf[Foo]),
          Groups.parse(Seq("id")),
          foo1InputOperator.getOperatorPort, foo2InputOperator.getOperatorPort)
        .input("bars", ClassDescription.of(classOf[Bar]),
          Groups.parse(Seq("fooId"), Seq("+id")),
          roundBar.findOutput("output"))
        .output("joined", ClassDescription.of(classOf[FooBar]))
        .output("missed", ClassDescription.of(classOf[Bar]))
        .build()

      val joinedOutputOperator = ExternalOutput
        .newInstance("joined", masterJoinOperator.findOutput("joined"))

      val missedOutputOperator = ExternalOutput
        .newInstance("missed", masterJoinOperator.findOutput("missed"))

      val graph = new OperatorGraph(Seq(
        foo1InputOperator, foo2InputOperator, barInputOperator,
        masterJoinOperator,
        joinedOutputOperator, missedOutputOperator))

      val executorType = compile(flowId, graph, 6, path, classServer.root.toFile)
      execute(flowId, executorType, rounds)

      for {
        round <- rounds
      } {
        {
          val found = readResult[FooBar](joinedOutputOperator.getName, round, path)
            .map { foobar =>
              (foobar.id.get, foobar.foo.getAsString, foobar.bar.getAsString)
            }.collect.toSeq.sortBy(_._1)
          assert(found.size === 5)
          assert(found ===
            (5 until 10).map(i => (100 * round + i, s"foo${100 * round + i}", s"bar${100 * round + 10 + i}")))
        }
        {
          val missed = readResult[Bar](missedOutputOperator.getName, round, path)
            .map { bar =>
              (bar.id.get, bar.fooId.get, bar.bar.getAsString)
            }.collect.toSeq.sortBy(_._1)
          assert(missed.size === 5)
          assert(missed ===
            (10 until 15).map(i => (100 * round + 10 + i, 100 * round + i, s"bar${100 * round + 10 + i}")))
        }
      }
    }
  }

  for {
    iterativeExtension <- Seq(
      new IterativeExtension(),
      new IterativeExtension("round"))
  } {
    val conf = s"${configuration}, IterativeExtension: ${iterativeExtension}"

    it should s"compile IterativeBatchExecutor with broadcast MasterJoin: [${conf}]" in { implicit sc =>
      val path = createTempDirectoryForEach("test-").toFile

      prepareData("foos", path) {
        sc.parallelize(0 until 10).map(Foo.intToFoo)
      }
      prepareData("bars", path) {
        sc.parallelize(5 until 15).map(Bar.intToBar)
      }

      val fooInputOperator = ExternalInput
        .newInstance("foos/part-*",
          new ExternalInputInfo.Basic(
            ClassDescription.of(classOf[Foo]),
            "foos",
            ClassDescription.of(classOf[Foo]),
            ExternalInputInfo.DataSize.TINY))

      val roundFoo = OperatorExtractor
        .extract(classOf[Update], classOf[Ops], "roundFoo")
        .input("foo", ClassDescription.of(classOf[Foo]), fooInputOperator.getOperatorPort)
        .output("output", ClassDescription.of(classOf[Foo]))
        .attribute(classOf[IterativeExtension], iterativeExtension)
        .build()

      val barInputOperator = ExternalInput
        .newInstance("bars/part-*",
          new ExternalInputInfo.Basic(
            ClassDescription.of(classOf[Bar]),
            "bars",
            ClassDescription.of(classOf[Bar]),
            ExternalInputInfo.DataSize.UNKNOWN))

      val roundBar = OperatorExtractor
        .extract(classOf[Update], classOf[Ops], "roundBar")
        .input("bar", ClassDescription.of(classOf[Bar]), barInputOperator.getOperatorPort)
        .output("output", ClassDescription.of(classOf[Bar]))
        .attribute(classOf[IterativeExtension], iterativeExtension)
        .build()

      val masterJoinOperator = OperatorExtractor
        .extract(classOf[MasterJoin], classOf[Ops], "masterjoin")
        .input("foos", ClassDescription.of(classOf[Foo]),
          Groups.parse(Seq("id")),
          roundFoo.findOutput("output"))
        .input("bars", ClassDescription.of(classOf[Bar]),
          Groups.parse(Seq("fooId"), Seq("+id")),
          roundBar.findOutput("output"))
        .output("joined", ClassDescription.of(classOf[FooBar]))
        .output("missed", ClassDescription.of(classOf[Bar]))
        .build()

      val foundOutputOperator = ExternalOutput
        .newInstance("joined", masterJoinOperator.findOutput("joined"))

      val missedOutputOperator = ExternalOutput
        .newInstance("missed", masterJoinOperator.findOutput("missed"))

      val graph = new OperatorGraph(Seq(
        fooInputOperator, barInputOperator,
        masterJoinOperator,
        foundOutputOperator, missedOutputOperator))

      val executorType = compile(flowId, graph, 4, path, classServer.root.toFile)

      val rounds = 0 to 1
      execute(flowId, executorType, rounds)

      for {
        round <- rounds
      } {
        {
          val found = readResult[FooBar](foundOutputOperator.getName, round, path)
            .map { foobar =>
              (foobar.id.get, foobar.foo.getAsString, foobar.bar.getAsString)
            }.collect.toSeq.sortBy(_._1)
          assert(found.size === 5)
          assert(found ===
            (5 until 10).map(i => (100 * round + i, s"foo${100 * round + i}", s"bar${100 * round + i + 10}")))
        }
        {
          val missed = readResult[Bar](missedOutputOperator.getName, round, path)
            .map { bar =>
              (bar.id.get, bar.fooId.get, bar.bar.getAsString)
            }.collect.toSeq.sortBy(_._1)
          assert(missed.size === 5)
          assert(missed ===
            (10 until 15).map(i => (100 * round + 10 + i, 100 * round + i, s"bar${100 * round + 10 + i}")))
        }
      }
    }

    it should s"compile IterativeBatchExecutor with broadcast MasterJoin with fixed master: [${conf}]" in { implicit sc =>
      val path = createTempDirectoryForEach("test-").toFile
      val rounds = 0 to 1

      prepareData(s"foos", path) {
        sc.parallelize(0 until 10).map(Foo.intToFoo).flatMap { foo =>
          rounds.iterator.map { round =>
            Foo.round(foo, round)
          }
        }
      }
      prepareData("bars", path) {
        sc.parallelize(5 until 15).map(Bar.intToBar)
      }

      val fooInputOperator = ExternalInput
        .newInstance("foos/part-*",
          new ExternalInputInfo.Basic(
            ClassDescription.of(classOf[Foo]),
            "foos",
            ClassDescription.of(classOf[Foo]),
            ExternalInputInfo.DataSize.TINY))

      val barInputOperator = ExternalInput
        .newInstance("bars/part-*",
          new ExternalInputInfo.Basic(
            ClassDescription.of(classOf[Bar]),
            "bars",
            ClassDescription.of(classOf[Bar]),
            ExternalInputInfo.DataSize.UNKNOWN))

      val roundBar = OperatorExtractor
        .extract(classOf[Update], classOf[Ops], "roundBar")
        .input("bar", ClassDescription.of(classOf[Bar]), barInputOperator.getOperatorPort)
        .output("output", ClassDescription.of(classOf[Bar]))
        .attribute(classOf[IterativeExtension], iterativeExtension)
        .build()

      val masterJoinOperator = OperatorExtractor
        .extract(classOf[MasterJoin], classOf[Ops], "masterjoin")
        .input("foos", ClassDescription.of(classOf[Foo]),
          Groups.parse(Seq("id")),
          fooInputOperator.getOperatorPort)
        .input("bars", ClassDescription.of(classOf[Bar]),
          Groups.parse(Seq("fooId"), Seq("+id")),
          roundBar.findOutput("output"))
        .output("joined", ClassDescription.of(classOf[FooBar]))
        .output("missed", ClassDescription.of(classOf[Bar]))
        .build()

      val foundOutputOperator = ExternalOutput
        .newInstance("joined", masterJoinOperator.findOutput("joined"))

      val missedOutputOperator = ExternalOutput
        .newInstance("missed", masterJoinOperator.findOutput("missed"))

      val graph = new OperatorGraph(Seq(
        fooInputOperator, barInputOperator,
        masterJoinOperator,
        foundOutputOperator, missedOutputOperator))

      val executorType = compile(flowId, graph, 4, path, classServer.root.toFile)
      execute(flowId, executorType, rounds)

      for {
        round <- rounds
      } {
        {
          val found = readResult[FooBar](foundOutputOperator.getName, round, path)
            .map { foobar =>
              (foobar.id.get, foobar.foo.getAsString, foobar.bar.getAsString)
            }.collect.toSeq.sortBy(_._1)
          assert(found.size === 5)
          assert(found ===
            (5 until 10).map(i => (100 * round + i, s"foo${100 * round + i}", s"bar${100 * round + i + 10}")))
        }
        {
          val missed = readResult[Bar](missedOutputOperator.getName, round, path)
            .map { bar =>
              (bar.id.get, bar.fooId.get, bar.bar.getAsString)
            }.collect.toSeq.sortBy(_._1)
          assert(missed.size === 5)
          assert(missed ===
            (10 until 15).map(i => (100 * round + 10 + i, 100 * round + i, s"bar${100 * round + 10 + i}")))
        }
      }
    }
  }

  for {
    iterativeExtension <- Seq(
      new IterativeExtension(),
      new IterativeExtension("round"))
  } {
    val conf = s"${configuration}, IterativeExtension: ${iterativeExtension}"

    it should s"compile IterativeBatchExecutor with broadcast self MasterCheck: [${conf}]" in { implicit sc =>
      val path = createTempDirectoryForEach("test-").toFile

      prepareData("foos", path) {
        sc.parallelize(0 until 10).map(Foo.intToFoo)
      }

      val fooInputOperator = ExternalInput
        .newInstance("foos/part-*",
          new ExternalInputInfo.Basic(
            ClassDescription.of(classOf[Foo]),
            "foos1",
            ClassDescription.of(classOf[Foo]),
            ExternalInputInfo.DataSize.TINY))

      val roundFoo = OperatorExtractor
        .extract(classOf[Update], classOf[Ops], "roundFoo")
        .input("foo", ClassDescription.of(classOf[Foo]), fooInputOperator.getOperatorPort)
        .output("output", ClassDescription.of(classOf[Foo]))
        .attribute(classOf[IterativeExtension], iterativeExtension)
        .build()

      val masterCheckOperator = OperatorExtractor
        .extract(classOf[MasterCheck], classOf[Ops], "mastercheck")
        .input("fooms", ClassDescription.of(classOf[Foo]),
          Groups.parse(Seq("id")),
          roundFoo.findOutput("output"))
        .input("foots", ClassDescription.of(classOf[Foo]),
          Groups.parse(Seq("id")),
          roundFoo.findOutput("output"))
        .output("found", ClassDescription.of(classOf[Foo]))
        .output("missed", ClassDescription.of(classOf[Foo]))
        .build()

      val foundOutputOperator = ExternalOutput
        .newInstance("found", masterCheckOperator.findOutput("found"))

      val missedOutputOperator = ExternalOutput
        .newInstance("missed", masterCheckOperator.findOutput("missed"))

      val graph = new OperatorGraph(Seq(
        fooInputOperator,
        masterCheckOperator,
        foundOutputOperator, missedOutputOperator))

      val executorType = compile(flowId, graph, 4, path, classServer.root.toFile)

      val rounds = 0 to 1
      execute(flowId, executorType, rounds)

      for {
        round <- rounds
      } {
        {
          val found = readResult[Foo](foundOutputOperator.getName, round, path)
            .map { foo =>
              (foo.id.get, foo.foo.getAsString)
            }.collect.toSeq.sortBy(_._1)
          assert(found.size === 10)
          assert(found === (0 until 10).map(i => (100 * round + i, s"foo${100 * round + i}")))
        }
        {
          val missed = readResult[Foo](missedOutputOperator.getName, round, path)
            .map { foo =>
              (foo.id.get, foo.foo.getAsString)
            }.collect.toSeq.sortBy(_._1)
          assert(missed.size === 0)
        }
      }
    }
  }

  for {
    iterativeExtension <- Seq(
      new IterativeExtension(),
      new IterativeExtension("round"))
  } {
    val conf = s"${configuration}, IterativeExtension: ${iterativeExtension}"

    it should s"compile IterativeBatchExecutor with Fold: [${conf}]" in { implicit sc =>
      val path = createTempDirectoryForEach("test-").toFile

      prepareData("bazs1", path) {
        sc.parallelize(0 until 50).map(Baz.intToBaz)
      }
      prepareData("bazs2", path) {
        sc.parallelize(50 until 100).map(Baz.intToBaz)
      }

      val baz1InputOperator = ExternalInput
        .newInstance("bazs1/part-*",
          new ExternalInputInfo.Basic(
            ClassDescription.of(classOf[Baz]),
            "baz1",
            ClassDescription.of(classOf[Baz]),
            ExternalInputInfo.DataSize.UNKNOWN))

      val roundBaz1 = OperatorExtractor
        .extract(classOf[Update], classOf[Ops], "roundBaz")
        .input("baz", ClassDescription.of(classOf[Baz]), baz1InputOperator.getOperatorPort)
        .output("output", ClassDescription.of(classOf[Baz]))
        .attribute(classOf[IterativeExtension], iterativeExtension)
        .build()

      val baz2InputOperator = ExternalInput
        .newInstance("bazs2/part-*",
          new ExternalInputInfo.Basic(
            ClassDescription.of(classOf[Baz]),
            "baz2",
            ClassDescription.of(classOf[Baz]),
            ExternalInputInfo.DataSize.UNKNOWN))

      val roundBaz2 = OperatorExtractor
        .extract(classOf[Update], classOf[Ops], "roundBaz")
        .input("baz", ClassDescription.of(classOf[Baz]), baz2InputOperator.getOperatorPort)
        .output("output", ClassDescription.of(classOf[Baz]))
        .attribute(classOf[IterativeExtension], iterativeExtension)
        .build()

      val foldOperator = OperatorExtractor
        .extract(classOf[Fold], classOf[Ops], "fold")
        .input("bazs", ClassDescription.of(classOf[Baz]),
          Groups.parse(Seq("id")),
          roundBaz1.findOutput("output"), roundBaz2.findOutput("output"))
        .output("result", ClassDescription.of(classOf[Baz]))
        .build()

      val resultOutputOperator = ExternalOutput
        .newInstance("result", foldOperator.findOutput("result"))

      val graph = new OperatorGraph(Seq(
        baz1InputOperator, baz2InputOperator,
        foldOperator,
        resultOutputOperator))

      val executorType = compile(flowId, graph, 4, path, classServer.root.toFile)

      val rounds = 0 to 1
      execute(flowId, executorType, rounds)

      for {
        round <- rounds
      } {
        {
          val result = readResult[Baz](resultOutputOperator.getName, round, path)
            .map { baz =>
              (baz.id.get, baz.n.get)
            }.collect.toSeq.sortBy(_._1)
          assert(result.size === 2)
          assert(result(0)._1 === 100 * round + 0)
          assert(result(0)._2 === (0 until 100 by 2).map(i => 100 * round + i * 100).sum)
          assert(result(1)._1 === 100 * round + 1)
          assert(result(1)._2 === (1 until 100 by 2).map(i => 100 * round + i * 100).sum)
        }
      }
    }
  }

  for {
    iterativeExtension <- Seq(
      new IterativeExtension(),
      new IterativeExtension("round"))
  } {
    val conf = s"${configuration}, IterativeExtension: ${iterativeExtension}"

    it should s"compile IterativeBatchExecutor with Fold with grouping is empty: [${conf}]" in { implicit sc =>
      val path = createTempDirectoryForEach("test-").toFile

      prepareData("bazs1", path) {
        sc.parallelize(0 until 50).map(Baz.intToBaz)
      }
      prepareData("bazs2", path) {
        sc.parallelize(50 until 100).map(Baz.intToBaz)
      }

      val baz1InputOperator = ExternalInput
        .newInstance("bazs1/part-*",
          new ExternalInputInfo.Basic(
            ClassDescription.of(classOf[Baz]),
            "baz1",
            ClassDescription.of(classOf[Baz]),
            ExternalInputInfo.DataSize.UNKNOWN))

      val roundBaz1 = OperatorExtractor
        .extract(classOf[Update], classOf[Ops], "roundBaz")
        .input("baz", ClassDescription.of(classOf[Baz]), baz1InputOperator.getOperatorPort)
        .output("output", ClassDescription.of(classOf[Baz]))
        .attribute(classOf[IterativeExtension], iterativeExtension)
        .build()

      val baz2InputOperator = ExternalInput
        .newInstance("bazs2/part-*",
          new ExternalInputInfo.Basic(
            ClassDescription.of(classOf[Baz]),
            "baz2",
            ClassDescription.of(classOf[Baz]),
            ExternalInputInfo.DataSize.UNKNOWN))

      val roundBaz2 = OperatorExtractor
        .extract(classOf[Update], classOf[Ops], "roundBaz")
        .input("baz", ClassDescription.of(classOf[Baz]), baz2InputOperator.getOperatorPort)
        .output("output", ClassDescription.of(classOf[Baz]))
        .attribute(classOf[IterativeExtension], iterativeExtension)
        .build()

      val foldOperator = OperatorExtractor
        .extract(classOf[Fold], classOf[Ops], "fold")
        .input("bazs", ClassDescription.of(classOf[Baz]),
          Groups.parse(Seq.empty[String]),
          roundBaz1.findOutput("output"), roundBaz2.findOutput("output"))
        .output("result", ClassDescription.of(classOf[Baz]))
        .build()

      val resultOutputOperator = ExternalOutput
        .newInstance("result", foldOperator.findOutput("result"))

      val graph = new OperatorGraph(Seq(
        baz1InputOperator, baz2InputOperator,
        foldOperator,
        resultOutputOperator))

      val executorType = compile(flowId, graph, 4, path, classServer.root.toFile)

      val rounds = 0 to 1
      execute(flowId, executorType, rounds)

      for {
        round <- rounds
      } {
        {
          val result = readResult[Baz](resultOutputOperator.getName, round, path)
            .map { baz =>
              (baz.id.get, baz.n.get)
            }.collect.toSeq.sortBy(_._1)
          assert(result.size === 1)
          assert(result(0)._2 === (0 until 100).map(i => 100 * round + i * 100).sum)
        }
      }
    }
  }

  for {
    iterativeExtension <- Seq(
      new IterativeExtension(),
      new IterativeExtension("round"))
  } {
    val conf = s"${configuration}, IterativeExtension: ${iterativeExtension}"

    it should s"compile IterativeBatchExecutor with Summarize: [${conf}]" in { implicit sc =>
      val path = createTempDirectoryForEach("test-").toFile

      prepareData("bazs1", path) {
        sc.parallelize(0 until 500).map(Baz.intToBaz)
      }
      prepareData("bazs2", path) {
        sc.parallelize(500 until 1000).map(Baz.intToBaz)
      }

      val baz1InputOperator = ExternalInput
        .newInstance("bazs1/part-*",
          new ExternalInputInfo.Basic(
            ClassDescription.of(classOf[Baz]),
            "baz1",
            ClassDescription.of(classOf[Baz]),
            ExternalInputInfo.DataSize.UNKNOWN))

      val roundBaz1 = OperatorExtractor
        .extract(classOf[Update], classOf[Ops], "roundBaz")
        .input("baz", ClassDescription.of(classOf[Baz]), baz1InputOperator.getOperatorPort)
        .output("output", ClassDescription.of(classOf[Baz]))
        .attribute(classOf[IterativeExtension], iterativeExtension)
        .build()

      val baz2InputOperator = ExternalInput
        .newInstance("bazs2/part-*",
          new ExternalInputInfo.Basic(
            ClassDescription.of(classOf[Baz]),
            "baz2",
            ClassDescription.of(classOf[Baz]),
            ExternalInputInfo.DataSize.UNKNOWN))

      val roundBaz2 = OperatorExtractor
        .extract(classOf[Update], classOf[Ops], "roundBaz")
        .input("baz", ClassDescription.of(classOf[Baz]), baz2InputOperator.getOperatorPort)
        .output("output", ClassDescription.of(classOf[Baz]))
        .attribute(classOf[IterativeExtension], iterativeExtension)
        .build()

      val summarizeOperator = OperatorExtractor
        .extract(classOf[Summarize], classOf[Ops], "summarize")
        .input("bazs", ClassDescription.of(classOf[Baz]),
          Groups.parse(Seq("id")),
          roundBaz1.findOutput("output"), roundBaz2.findOutput("output"))
        .output("result", ClassDescription.of(classOf[SummarizedBaz]))
        .build()

      val resultOutputOperator = ExternalOutput
        .newInstance("result", summarizeOperator.findOutput("result"))

      val graph = new OperatorGraph(Seq(
        baz1InputOperator, baz2InputOperator,
        summarizeOperator,
        resultOutputOperator))

      val executorType = compile(flowId, graph, 4, path, classServer.root.toFile)

      val rounds = 0 to 1
      execute(flowId, executorType, rounds)

      for {
        round <- rounds
      } {
        {
          val result = readResult[SummarizedBaz](resultOutputOperator.getName, round, path)
            .map { baz =>
              (baz.id.get, baz.sum.get, baz.max.get, baz.min.get, baz.count.get)
            }.collect.toSeq.sortBy(_._1)
          assert(result.size === 2)
          assert(result(0)._1 === 100 * round + 0)
          assert(result(0)._2 === (0 until 1000 by 2).map(i => 100 * round + i * 100).sum)
          assert(result(0)._3 === 100 * round + 99800)
          assert(result(0)._4 === 100 * round + 0)
          assert(result(0)._5 === 500)
          assert(result(1)._1 === 100 * round + 1)
          assert(result(1)._2 === (1 until 1000 by 2).map(i => 100 * round + i * 100).sum)
          assert(result(1)._3 === 100 * round + 99900)
          assert(result(1)._4 === 100 * round + 100)
          assert(result(1)._5 === 500)
        }
      }
    }
  }

  for {
    iterativeExtension <- Seq(
      new IterativeExtension(),
      new IterativeExtension("round"))
  } {
    val conf = s"${configuration}, IterativeExtension: ${iterativeExtension}"

    it should s"compile IterativeBatchExecutor with Summarize with grouping is empty: [${conf}]" in { implicit sc =>
      val path = createTempDirectoryForEach("test-").toFile

      prepareData("bazs1", path) {
        sc.parallelize(0 until 500).map(Baz.intToBaz)
      }
      prepareData("bazs2", path) {
        sc.parallelize(500 until 1000).map(Baz.intToBaz)
      }

      val baz1InputOperator = ExternalInput
        .newInstance("bazs1/part-*",
          new ExternalInputInfo.Basic(
            ClassDescription.of(classOf[Baz]),
            "baz1",
            ClassDescription.of(classOf[Baz]),
            ExternalInputInfo.DataSize.UNKNOWN))

      val roundBaz1 = OperatorExtractor
        .extract(classOf[Update], classOf[Ops], "roundBaz")
        .input("baz", ClassDescription.of(classOf[Baz]), baz1InputOperator.getOperatorPort)
        .output("output", ClassDescription.of(classOf[Baz]))
        .attribute(classOf[IterativeExtension], iterativeExtension)
        .build()

      val baz2InputOperator = ExternalInput
        .newInstance("bazs2/part-*",
          new ExternalInputInfo.Basic(
            ClassDescription.of(classOf[Baz]),
            "baz2",
            ClassDescription.of(classOf[Baz]),
            ExternalInputInfo.DataSize.UNKNOWN))

      val roundBaz2 = OperatorExtractor
        .extract(classOf[Update], classOf[Ops], "roundBaz")
        .input("baz", ClassDescription.of(classOf[Baz]), baz2InputOperator.getOperatorPort)
        .output("output", ClassDescription.of(classOf[Baz]))
        .attribute(classOf[IterativeExtension], iterativeExtension)
        .build()

      val summarizeOperator = OperatorExtractor
        .extract(classOf[Summarize], classOf[Ops], "summarize")
        .input("bazs", ClassDescription.of(classOf[Baz]),
          Groups.parse(Seq.empty[String]),
          roundBaz1.findOutput("output"), roundBaz2.findOutput("output"))
        .output("result", ClassDescription.of(classOf[SummarizedBaz]))
        .build()

      val resultOutputOperator = ExternalOutput
        .newInstance("result", summarizeOperator.findOutput("result"))

      val graph = new OperatorGraph(Seq(
        baz1InputOperator, baz2InputOperator,
        summarizeOperator,
        resultOutputOperator))

      val executorType = compile(flowId, graph, 4, path, classServer.root.toFile)

      val rounds = 0 to 1
      execute(flowId, executorType, rounds)

      for {
        round <- rounds
      } {
        {
          val result = readResult[SummarizedBaz](resultOutputOperator.getName, round, path)
            .map { baz =>
              (baz.id.get, baz.sum.get, baz.max.get, baz.min.get, baz.count.get)
            }.collect.toSeq.sortBy(_._1)
          assert(result.size === 1)
          assert(result(0)._2 === (0 until 1000).map(i => 100 * round + i * 100).sum)
          assert(result(0)._3 === 100 * round + 99900)
          assert(result(0)._4 === 100 * round + 0)
          assert(result(0)._5 === 1000)
        }
      }
    }
  }

  def newJPContext(
    path: File,
    classpath: File,
    properties: Map[String, String] = Map.empty): MockJobflowProcessorContext = {
    val jpContext = new MockJobflowProcessorContext(
      new CompilerOptions("buildid", path.getPath, properties),
      Thread.currentThread.getContextClassLoader,
      classpath)
    jpContext.registerExtension(
      classOf[InspectionExtension],
      new AbstractInspectionExtension {

        override def addResource(location: Location) = {
          jpContext.addResourceFile(location)
        }
      })
    jpContext
  }

  def newJobflow(flowId: String, graph: OperatorGraph): Jobflow = {
    new Jobflow(flowId, ClassDescription.of(classOf[IterativeBatchExecutorCompilerSpec]), graph)
  }

  def compile(
    flowId: String,
    graph: OperatorGraph,
    subplans: Int, path: File,
    classpath: File,
    properties: Map[String, String] = Map.empty): Type = {
    val jpContext = newJPContext(path, classpath, properties)
    val jobflow = newJobflow(flowId, graph)
    val plan = SparkPlanning.plan(jpContext, jobflow).getPlan
    assert(plan.getElements.size === subplans)

    implicit val context = newIterativeBatchExecutorCompilerContext(flowId, jpContext)
    val executorType = IterativeBatchExecutorCompiler.compile(plan)
    context.addClass(context.branchKeys)
    context.addClass(context.broadcastIds)
    executorType
  }

  def execute(flowId: String, executorType: Type, rounds: Seq[Int])(implicit sc: SparkContext): Unit = {
    val cls = Class.forName(
      executorType.getClassName, true, Thread.currentThread.getContextClassLoader)
      .asSubclass(classOf[IterativeBatchExecutor])
    val executor = cls.getConstructor(classOf[Int], classOf[ExecutionContext], classOf[SparkContext])
      .newInstance(Int.box(1), ExecutionContext.global, sc)

    try {
      executor.start()
      executor.submitAll(rounds.map { round =>
        newRoundContext(
          flowId = flowId,
          stageId = s"round_${round}",
          batchArguments = Map("round" -> round.toString))
      })
    } finally {
      executor.stop(awaitExecution = true, gracefully = true)
    }
  }
}

object IterativeBatchExecutorCompilerSpec {

  class Foo extends DataModel[Foo] with Writable {

    val id = new IntOption()
    val foo = new StringOption()

    override def reset(): Unit = {
      id.setNull()
      foo.setNull()
    }
    override def copyFrom(other: Foo): Unit = {
      id.copyFrom(other.id)
      foo.copyFrom(other.foo)
    }
    override def readFields(in: DataInput): Unit = {
      id.readFields(in)
      foo.readFields(in)
    }
    override def write(out: DataOutput): Unit = {
      id.write(out)
      foo.write(out)
    }

    def getIdOption: IntOption = id
    def getFooOption: StringOption = foo
  }

  object Foo {

    def intToFoo: Int => Foo = {

      lazy val foo = new Foo()

      { i =>
        foo.id.modify(i)
        foo.foo.modify(s"foo${i}")
        foo
      }
    }

    def round(foo: Foo, round: Int): Foo = {
      val id = foo.id.get
      foo.id.modify(100 * round + id)
      foo.foo.modify(s"foo${100 * round + id}")
      foo
    }
  }

  class Bar extends DataModel[Bar] with Writable {

    val id = new IntOption()
    val fooId = new IntOption()
    val bar = new StringOption()

    override def reset(): Unit = {
      id.setNull()
      fooId.setNull()
      bar.setNull()
    }
    override def copyFrom(other: Bar): Unit = {
      id.copyFrom(other.id)
      fooId.copyFrom(other.fooId)
      bar.copyFrom(other.bar)
    }
    override def readFields(in: DataInput): Unit = {
      id.readFields(in)
      fooId.readFields(in)
      bar.readFields(in)
    }
    override def write(out: DataOutput): Unit = {
      id.write(out)
      fooId.write(out)
      bar.write(out)
    }

    def getIdOption: IntOption = id
    def getFooIdOption: IntOption = fooId
    def getBarOption: StringOption = bar
  }

  object Bar {

    def intToBar: Int => Bar = {

      lazy val bar = new Bar()

      { i =>
        bar.id.modify(10 + i)
        bar.fooId.modify(i)
        bar.bar.modify(s"bar${10 + i}")
        bar
      }
    }

    def intToBars: Int => Iterator[Bar] = {

      lazy val bar = new Bar()

      { i =>
        (0 until i).iterator.map { j =>
          bar.id.modify(10 + j)
          bar.fooId.modify(i)
          bar.bar.modify(s"bar${10 + j}")
          bar
        }
      }
    }

    def round(bar: Bar, round: Int): Bar = {
      val id = bar.id.get
      bar.id.modify(100 * round + id)
      bar.fooId.modify(100 * round + bar.fooId.get)
      bar.bar.modify(s"bar${100 * round + id}")
      bar
    }
  }

  @Joined(terms = Array(
    new Joined.Term(source = classOf[Foo], shuffle = new Key(group = Array("id")), mappings = Array(
      new Joined.Mapping(source = "id", destination = "id"),
      new Joined.Mapping(source = "foo", destination = "foo"))),
    new Joined.Term(source = classOf[Bar], shuffle = new Key(group = Array("fooId")), mappings = Array(
      new Joined.Mapping(source = "fooId", destination = "id"),
      new Joined.Mapping(source = "bar", destination = "bar")))))
  class FooBar extends DataModel[FooBar] with Writable {

    val id = new IntOption()
    val foo = new StringOption()
    val bar = new StringOption()

    override def reset(): Unit = {
      id.setNull()
      foo.setNull()
      bar.setNull()
    }
    override def copyFrom(other: FooBar): Unit = {
      id.copyFrom(other.id)
      foo.copyFrom(other.foo)
      bar.copyFrom(other.bar)
    }
    override def readFields(in: DataInput): Unit = {
      id.readFields(in)
      foo.readFields(in)
      bar.readFields(in)
    }
    override def write(out: DataOutput): Unit = {
      id.write(out)
      foo.write(out)
      bar.write(out)
    }

    def getIdOption: IntOption = id
    def getFooOption: StringOption = foo
    def getBarOption: StringOption = bar
  }

  class Baz extends DataModel[Baz] with Writable {

    val id = new IntOption()
    val n = new IntOption()

    override def reset(): Unit = {
      id.setNull()
      n.setNull()
    }
    override def copyFrom(other: Baz): Unit = {
      id.copyFrom(other.id)
      n.copyFrom(other.n)
    }
    override def readFields(in: DataInput): Unit = {
      id.readFields(in)
      n.readFields(in)
    }
    override def write(out: DataOutput): Unit = {
      id.write(out)
      n.write(out)
    }

    def getIdOption: IntOption = id
    def getNOption: IntOption = n
  }

  object Baz {

    def intToBaz: Int => Baz = {

      lazy val baz = new Baz()

      { i =>
        baz.id.modify(i % 2)
        baz.n.modify(100 * i)
        baz
      }
    }

    def round(baz: Baz, round: Int): Baz = {
      baz.id.modify(100 * round + baz.id.get)
      baz.n.modify(100 * round + baz.n.get)
      baz
    }
  }

  @Summarized(term = new Summarized.Term(
    source = classOf[Baz],
    shuffle = new Key(group = Array("id")),
    foldings = Array(
      new Summarized.Folding(source = "id", destination = "id", aggregator = Summarized.Aggregator.ANY),
      new Summarized.Folding(source = "n", destination = "sum", aggregator = Summarized.Aggregator.SUM),
      new Summarized.Folding(source = "n", destination = "max", aggregator = Summarized.Aggregator.MAX),
      new Summarized.Folding(source = "n", destination = "min", aggregator = Summarized.Aggregator.MIN),
      new Summarized.Folding(source = "id", destination = "count", aggregator = Summarized.Aggregator.COUNT))))
  class SummarizedBaz extends DataModel[SummarizedBaz] with Writable {

    val id = new IntOption()
    val sum = new LongOption()
    val max = new IntOption()
    val min = new IntOption()
    val count = new LongOption()

    override def reset(): Unit = {
      id.setNull()
      sum.setNull()
      max.setNull()
      min.setNull()
      count.setNull()
    }
    override def copyFrom(other: SummarizedBaz): Unit = {
      id.copyFrom(other.id)
      sum.copyFrom(other.sum)
      max.copyFrom(other.max)
      min.copyFrom(other.min)
      count.copyFrom(other.count)
    }
    override def readFields(in: DataInput): Unit = {
      id.readFields(in)
      sum.readFields(in)
      max.readFields(in)
      min.readFields(in)
      count.readFields(in)
    }
    override def write(out: DataOutput): Unit = {
      id.write(out)
      sum.write(out)
      max.write(out)
      min.write(out)
      count.write(out)
    }

    def getIdOption: IntOption = id
    def getSumOption: LongOption = sum
    def getMaxOption: IntOption = max
    def getMinOption: IntOption = min
    def getCountOption: LongOption = count
  }

  class Ops {

    @Update
    def roundFoo(foo: Foo): Unit = {
      val round = BatchContext.get("round").toInt
      Foo.round(foo, round)
    }

    @Update
    def roundBar(bar: Bar): Unit = {
      val round = BatchContext.get("round").toInt
      Bar.round(bar, round)
    }

    @Update
    def roundBaz(baz: Baz): Unit = {
      val round = BatchContext.get("round").toInt
      Baz.round(baz, round)
    }

    @Logging(Logging.Level.INFO)
    def logging(foo: Foo): String = {
      s"Foo(${foo.id},${foo.foo})"
    }

    @Extract
    def extract(foo: Foo, evenResult: Result[Foo], oddResult: Result[Foo]): Unit = {
      if (foo.id.get % 2 == 0) {
        evenResult.add(foo)
      } else {
        oddResult.add(foo)
      }
    }

    @CoGroup
    def cogroup(
      foos: JList[Foo], bars: JList[Bar],
      fooResult: Result[Foo], barResult: Result[Bar],
      fooError: Result[Foo], barError: Result[Bar]): Unit = {
      if (foos.size == 1 && bars.size == 1) {
        fooResult.add(foos(0))
        barResult.add(bars(0))
      } else {
        foos.foreach(fooError.add)
        bars.foreach(barError.add)
      }
    }

    @MasterCheck
    def mastercheck(foo: Foo, bar: Bar): Boolean = ???

    @MasterJoin
    def masterjoin(foo: Foo, bar: Bar): FooBar = ???

    @Fold(partialAggregation = PartialAggregation.PARTIAL)
    def fold(acc: Baz, each: Baz): Unit = {
      acc.n.add(each.n)
    }

    @Summarize
    def summarize(value: Baz): SummarizedBaz = ???
  }

  class FooSequenceFileFormat extends SequenceFileFormat[NullWritable, Foo, Foo] {

    override def getSupportedType(): Class[Foo] = classOf[Foo]

    override def createKeyObject(): NullWritable = NullWritable.get()

    override def createValueObject(): Foo = new Foo()

    override def copyToModel(key: NullWritable, value: Foo, model: Foo): Unit = {
      model.copyFrom(value)
    }

    override def copyFromModel(model: Foo, key: NullWritable, value: Foo): Unit = {
      value.copyFrom(model)
    }
  }
}
