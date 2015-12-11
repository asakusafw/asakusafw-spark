/*
 * Copyright 2011-2015 Asakusa Framework Team.
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
package com.asakusafw.spark.compiler

import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner

import java.io.{ File, DataInput, DataOutput }
import java.net.URLClassLoader
import java.util.{ List => JList }

import scala.collection.JavaConversions._
import scala.reflect.{ classTag, ClassTag }

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{ NullWritable, Writable }
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.rdd.RDD

import com.asakusafw.bridge.stage.StageInfo
import com.asakusafw.iterative.common.IterativeExtensions
import com.asakusafw.iterative.launch.IterativeStageInfo
import com.asakusafw.lang.compiler.api.CompilerOptions
import com.asakusafw.lang.compiler.api.JobflowProcessor.{ Context => JPContext }
import com.asakusafw.lang.compiler.api.testing.MockJobflowProcessorContext
import com.asakusafw.lang.compiler.common.Location
import com.asakusafw.lang.compiler.hadoop.{ InputFormatInfo, InputFormatInfoExtension }
import com.asakusafw.lang.compiler.inspection.{ AbstractInspectionExtension, InspectionExtension }
import com.asakusafw.lang.compiler.model.PropertyName
import com.asakusafw.lang.compiler.model.description.ClassDescription
import com.asakusafw.lang.compiler.model.graph._
import com.asakusafw.lang.compiler.model.info.ExternalInputInfo
import com.asakusafw.lang.compiler.model.testing.OperatorExtractor
import com.asakusafw.lang.compiler.planning._
import com.asakusafw.runtime.compatibility.JobCompatibility
import com.asakusafw.runtime.core.Result
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.runtime.stage.StageConstants
import com.asakusafw.runtime.stage.input.TemporaryInputFormat
import com.asakusafw.runtime.stage.output.TemporaryOutputFormat
import com.asakusafw.runtime.value._
import com.asakusafw.spark.runtime._
import com.asakusafw.spark.tools.asm._
import com.asakusafw.vocabulary.flow.processor.PartialAggregation
import com.asakusafw.vocabulary.model.{ Joined, Key, Summarized }
import com.asakusafw.vocabulary.operator._

@RunWith(classOf[JUnitRunner])
class SparkClientCompilerSpecTest extends SparkClientCompilerSpec

class SparkClientCompilerSpec extends FlatSpec with LoadClassSugar with TempDirForEach {

  import SparkClientCompilerSpec._

  behavior of classOf[SparkClientCompiler].getSimpleName

  for {
    threshold <- Seq(None, Some(0))
    parallelism <- Seq(None, Some(8), Some(0))
  } {
    val configuration = "[" +
      s"master=local[8]" +
      s"${threshold.map(t => s",threshold=${t}").getOrElse("")}" +
      s"${parallelism.map(p => s",parallelism=${p}").getOrElse("")}" +
      "]"

    def createTempDirs(): (File, File) = {
      val tmpDir = createTempDirectoryForEach("test-").toFile
      val classpath = new File(tmpDir, "classes").getAbsoluteFile
      classpath.mkdirs()
      val path = new File(tmpDir, "tmp").getAbsoluteFile
      (path, classpath)
    }

    def prepareData[T: ClassTag](name: String, path: File)(rdd: RDD[T])(implicit sc: SparkContext): Unit = {
      val job = JobCompatibility.newJob(sc.hadoopConfiguration)
      job.setOutputKeyClass(classOf[NullWritable])
      job.setOutputValueClass(classTag[T].runtimeClass)
      job.setOutputFormatClass(classOf[TemporaryOutputFormat[T]])
      TemporaryOutputFormat.setOutputPath(job, new Path(path.getPath, s"${MockJobflowProcessorContext.EXTERNAL_INPUT_BASE}${name}"))
      rdd.map((NullWritable.get, _)).saveAsNewAPIHadoopDataset(job.getConfiguration)
    }

    def readResult[T: ClassTag](name: String, path: File)(implicit sc: SparkContext): RDD[T] = {
      val job = JobCompatibility.newJob(sc.hadoopConfiguration)
      TemporaryInputFormat.setInputPaths(job, Seq(new Path(path.getPath, s"${name}/part-*")))
      sc.newAPIHadoopRDD(
        job.getConfiguration,
        classOf[TemporaryInputFormat[T]],
        classOf[NullWritable],
        classTag[T].runtimeClass.asInstanceOf[Class[T]]).map(_._2)
    }

    it should s"compile Spark client from simple plan: ${configuration}" in {
      val (path, classpath) = createTempDirs()

      spark { implicit sc =>
        prepareData("foo", path) {
          sc.parallelize(0 until 100).map(Foo.intToFoo)
        }
      }

      val inputOperator = ExternalInput
        .newInstance("foo/part-*",
          new ExternalInputInfo.Basic(
            ClassDescription.of(classOf[Foo]),
            "test",
            ClassDescription.of(classOf[Foo]),
            ExternalInputInfo.DataSize.UNKNOWN))

      val outputOperator = ExternalOutput
        .newInstance("output", inputOperator.getOperatorPort)

      val graph = new OperatorGraph(Seq(inputOperator, outputOperator))

      compile(graph, 2, path, classpath)
      execute(classpath, threshold, parallelism)

      spark { implicit sc =>
        val result = readResult[Foo]("output", path)
          .map { foo =>
            (foo.id.get, foo.foo.getAsString)
          }.collect.toSeq.sortBy(_._1)
        assert(result === (0 until 100).map(i => (i, s"foo${i}")))
      }
    }

    it should s"compile Spark client from simple plan with InputFormatInfo: ${configuration}" in {
      val (path, classpath) = createTempDirs()

      spark { implicit sc =>
        prepareData("foo", path) {
          sc.parallelize(0 until 100).map(Foo.intToFoo)
        }
      }

      val inputOperator = ExternalInput
        .newInstance("foo/part-*",
          new ExternalInputInfo.Basic(
            ClassDescription.of(classOf[Foo]),
            "test",
            ClassDescription.of(classOf[Foo]),
            ExternalInputInfo.DataSize.UNKNOWN))

      val outputOperator = ExternalOutput
        .newInstance("output", inputOperator.getOperatorPort)

      val graph = new OperatorGraph(Seq(inputOperator, outputOperator))

      { // compile
        val jpContext = newJPContext(path, classpath)
        jpContext.registerExtension(
          classOf[InputFormatInfoExtension],
          new InputFormatInfoExtension() {

            override def resolve(name: String, info: ExternalInputInfo): InputFormatInfo = {
              new InputFormatInfo(
                ClassDescription.of(classOf[TemporaryInputFormat[_]]),
                ClassDescription.of(classOf[NullWritable]),
                ClassDescription.of(classOf[Foo]),
                Map(FileInputFormat.INPUT_DIR -> s"${path.getPath}/external/input/foo/part-*"))
            }
          })
        val jobflow = newJobflow(graph)

        val compiler = newCompiler(2)
        compiler.process(jpContext, jobflow)
      }

      execute(classpath, threshold, parallelism)

      spark { implicit sc =>
        val result = readResult[Foo]("output", path)
          .map { foo =>
            (foo.id.get, foo.foo.getAsString)
          }.collect.toSeq.sortBy(_._1)
        assert(result === (0 until 100).map(i => (i, s"foo${i}")))
      }
    }

    it should s"compile Spark client with Logging: ${configuration}" in {
      val (path, classpath) = createTempDirs()

      spark { implicit sc =>
        prepareData("foo", path) {
          sc.parallelize(0 until 100).map(Foo.intToFoo)
        }
      }

      val inputOperator = ExternalInput
        .newInstance("foo/part-*",
          new ExternalInputInfo.Basic(
            ClassDescription.of(classOf[Foo]),
            "test",
            ClassDescription.of(classOf[Foo]),
            ExternalInputInfo.DataSize.UNKNOWN))

      val loggingOperator = OperatorExtractor
        .extract(classOf[Logging], classOf[Ops], "logging")
        .input("foo", ClassDescription.of(classOf[Foo]), inputOperator.getOperatorPort)
        .output("output", ClassDescription.of(classOf[Foo]))
        .build()

      val outputOperator = ExternalOutput
        .newInstance("output", loggingOperator.findOutput("output"))

      val graph = new OperatorGraph(Seq(inputOperator, loggingOperator, outputOperator))

      compile(graph, 2, path, classpath)
      execute(classpath, threshold, parallelism)

      spark { implicit sc =>
        val result = readResult[Foo]("output", path)
          .map { foo =>
            (foo.id.get, foo.foo.getAsString)
          }.collect.toSeq.sortBy(_._1)
        assert(result === (0 until 100).map(i => (i, s"foo${i}")))
      }
    }

    it should s"compile Spark client with Extract: ${configuration}" in {
      val (path, classpath) = createTempDirs()

      spark { implicit sc =>
        prepareData("foo", path) {
          sc.parallelize(0 until 100).map(Foo.intToFoo)
        }
      }

      val inputOperator = ExternalInput
        .newInstance("foo/part-*",
          new ExternalInputInfo.Basic(
            ClassDescription.of(classOf[Foo]),
            "test",
            ClassDescription.of(classOf[Foo]),
            ExternalInputInfo.DataSize.UNKNOWN))

      val extractOperator = OperatorExtractor
        .extract(classOf[Extract], classOf[Ops], "extract")
        .input("foo", ClassDescription.of(classOf[Foo]), inputOperator.getOperatorPort)
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

      compile(graph, 3, path, classpath)
      execute(classpath, threshold, parallelism)

      spark { implicit sc =>
        {
          val result = readResult[Foo]("even", path)
            .map { foo =>
              (foo.id.get, foo.foo.getAsString)
            }.collect.toSeq.sortBy(_._1)
          assert(result === (0 until 100).filter(_ % 2 == 0).map(i => (i, s"foo${i}")))
        }
        {
          val result = readResult[Foo]("odd", path)
            .map { foo =>
              (foo.id.get, foo.foo.getAsString)
            }.collect.toSeq.sortBy(_._1)
          assert(result === (0 until 100).filterNot(_ % 2 == 0).map(i => (i, s"foo${i}")))
        }
      }
    }

    it should s"compile Spark client with Checkpoint and Extract: ${configuration}" in {
      val (path, classpath) = createTempDirs()

      spark { implicit sc =>
        prepareData("foo", path) {
          sc.parallelize(0 until 100).map(Foo.intToFoo)
        }
      }

      val inputOperator = ExternalInput
        .newInstance("foo/part-*",
          new ExternalInputInfo.Basic(
            ClassDescription.of(classOf[Foo]),
            "test",
            ClassDescription.of(classOf[Foo]),
            ExternalInputInfo.DataSize.UNKNOWN))

      val checkpointOperator = CoreOperator
        .builder(CoreOperator.CoreOperatorKind.CHECKPOINT)
        .input("input", ClassDescription.of(classOf[Foo]), inputOperator.getOperatorPort)
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

      compile(graph, 4, path, classpath)
      execute(classpath, threshold, parallelism)

      spark { implicit sc =>
        {
          val result = readResult[Foo]("even", path)
            .map { foo =>
              (foo.id.get, foo.foo.getAsString)
            }.collect.toSeq.sortBy(_._1)
          assert(result === (0 until 100).filter(_ % 2 == 0).map(i => (i, s"foo${i}")))
        }
        {
          val result = readResult[Foo]("odd", path)
            .map { foo =>
              (foo.id.get, foo.foo.getAsString)
            }.collect.toSeq.sortBy(_._1)
          assert(result === (0 until 100).filterNot(_ % 2 == 0).map(i => (i, s"foo${i}")))
        }
      }
    }

    it should s"compile Spark client with CoGroup: ${configuration}" in {
      val (path, classpath) = createTempDirs()

      spark { implicit sc =>
        prepareData("foo1", path) {
          sc.parallelize(0 until 5).map(Foo.intToFoo)
        }
        prepareData("foo2", path) {
          sc.parallelize(5 until 10).map(Foo.intToFoo)
        }
        prepareData("bar", path) {
          sc.parallelize(0 until 10).flatMap(Bar.intToBars)
        }
      }

      val fooInputOperator = ExternalInput
        .newInstance("foo1/part-*",
          new ExternalInputInfo.Basic(
            ClassDescription.of(classOf[Foo]),
            "foos1",
            ClassDescription.of(classOf[Foo]),
            ExternalInputInfo.DataSize.UNKNOWN))

      val foo2InputOperator = ExternalInput
        .newInstance("foo2/part-*",
          new ExternalInputInfo.Basic(
            ClassDescription.of(classOf[Foo]),
            "foos2",
            ClassDescription.of(classOf[Foo]),
            ExternalInputInfo.DataSize.UNKNOWN))

      val barInputOperator = ExternalInput
        .newInstance("bar/part-*",
          new ExternalInputInfo.Basic(
            ClassDescription.of(classOf[Bar]),
            "bars",
            ClassDescription.of(classOf[Bar]),
            ExternalInputInfo.DataSize.UNKNOWN))

      val cogroupOperator = OperatorExtractor
        .extract(classOf[CoGroup], classOf[Ops], "cogroup")
        .input("foos", ClassDescription.of(classOf[Foo]),
          Groups.parse(Seq("id")),
          fooInputOperator.getOperatorPort, foo2InputOperator.getOperatorPort)
        .input("bars", ClassDescription.of(classOf[Bar]),
          Groups.parse(Seq("fooId"), Seq("+id")),
          barInputOperator.getOperatorPort)
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
        fooInputOperator, foo2InputOperator, barInputOperator,
        cogroupOperator,
        fooResultOutputOperator, barResultOutputOperator, fooErrorOutputOperator, barErrorOutputOperator))

      compile(graph, 8, path, classpath)
      execute(classpath, threshold, parallelism)

      spark { implicit sc =>
        {
          val fooResult = readResult[Foo]("fooResult", path)
            .map { foo =>
              (foo.id.get, foo.foo.getAsString)
            }.collect.toSeq.sortBy(_._1)
          assert(fooResult.size === 1)
          assert(fooResult(0) === (1, "foo1"))
        }
        {
          val barResult = readResult[Bar]("barResult", path)
            .map { bar =>
              (bar.id.get, bar.fooId.get, bar.bar.getAsString)
            }.collect.toSeq.sortBy(_._1)
          assert(barResult.size === 1)
          assert(barResult(0) === (10, 1, "bar10"))
        }
        {
          val fooError = readResult[Foo]("fooError", path)
            .map { foo =>
              (foo.id.get, foo.foo.getAsString)
            }.collect.toSeq.sortBy(_._1)
          assert(fooError.size === 9)
          assert(fooError(0) === (0, "foo0"))
          for (i <- 2 until 10) {
            assert(fooError(i - 1) === (i, s"foo${i}"))
          }
        }
        {
          val barError = readResult[Bar]("barError", path)
            .map { bar =>
              (bar.id.get, bar.fooId.get, bar.bar.getAsString)
            }.collect.toSeq.sortBy(bar => (bar._2, bar._1))
          assert(barError.size === 44)
          for {
            i <- 2 until 10
            j <- 0 until i
          } {
            assert(barError((i * (i - 1)) / 2 + j - 1) === (10 + j, i, s"bar${10 + j}"))
          }
        }
      }
    }

    it should s"compile Spark client with CoGroup with grouping is empty: ${configuration}" in {
      val (path, classpath) = createTempDirs()

      spark { implicit sc =>
        prepareData("foo1", path) {
          sc.parallelize(0 until 5).map(Foo.intToFoo)
        }
        prepareData("foo2", path) {
          sc.parallelize(5 until 10).map(Foo.intToFoo)
        }
        prepareData("bar", path) {
          sc.parallelize(0 until 10).flatMap(Bar.intToBars)
        }
      }

      val foo1InputOperator = ExternalInput
        .newInstance("foo1/part-*",
          new ExternalInputInfo.Basic(
            ClassDescription.of(classOf[Foo]),
            "foos1",
            ClassDescription.of(classOf[Foo]),
            ExternalInputInfo.DataSize.UNKNOWN))

      val foo2InputOperator = ExternalInput
        .newInstance("foo2/part-*",
          new ExternalInputInfo.Basic(
            ClassDescription.of(classOf[Foo]),
            "foos2",
            ClassDescription.of(classOf[Foo]),
            ExternalInputInfo.DataSize.UNKNOWN))

      val barInputOperator = ExternalInput
        .newInstance("bar/part-*",
          new ExternalInputInfo.Basic(
            ClassDescription.of(classOf[Bar]),
            "bars",
            ClassDescription.of(classOf[Bar]),
            ExternalInputInfo.DataSize.UNKNOWN))

      val cogroupOperator = OperatorExtractor
        .extract(classOf[CoGroup], classOf[Ops], "cogroup")
        .input("foos", ClassDescription.of(classOf[Foo]),
          Groups.parse(Seq.empty[String]),
          foo1InputOperator.getOperatorPort, foo2InputOperator.getOperatorPort)
        .input("bars", ClassDescription.of(classOf[Bar]),
          Groups.parse(Seq.empty[String], Seq("+id")),
          barInputOperator.getOperatorPort)
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

      compile(graph, 8, path, classpath)
      execute(classpath, threshold, parallelism)

      spark { implicit sc =>
        {
          val fooResult = readResult[Foo]("fooResult", path)
            .map { foo =>
              (foo.id.get, foo.foo.getAsString)
            }.collect.toSeq.sortBy(_._1)
          assert(fooResult.size === 0)
        }
        {
          val barResult = readResult[Bar]("barResult", path)
            .map { bar =>
              (bar.id.get, bar.fooId.get, bar.bar.getAsString)
            }.collect.toSeq.sortBy(_._1)
          assert(barResult.size === 0)
        }
        {
          val fooError = readResult[Foo]("fooError", path)
            .map { foo =>
              (foo.id.get, foo.foo.getAsString)
            }.collect.toSeq.sortBy(_._1)
          assert(fooError.size === 10)
          for (i <- 0 until 10) {
            assert(fooError(i) === (i, s"foo${i}"))
          }
        }
        {
          val barError = readResult[Bar]("barError", path)
            .map { bar =>
              (bar.id.get, bar.fooId.get, bar.bar.getAsString)
            }.collect.toSeq.sortBy(bar => (bar._2, bar._1))
          assert(barError.size === 45)
          for {
            i <- 0 until 10
            j <- 0 until i
          } {
            assert(barError((i * (i - 1)) / 2 + j) === (10 + j, i, s"bar${10 + j}"))
          }
        }
      }
    }

    it should s"compile Spark client with MasterCheck: ${configuration}" in {
      val (path, classpath) = createTempDirs()

      spark { implicit sc =>
        prepareData("foo1", path) {
          sc.parallelize(0 until 5).map(Foo.intToFoo)
        }
        prepareData("foo2", path) {
          sc.parallelize(5 until 10).map(Foo.intToFoo)
        }
        prepareData("bar", path) {
          sc.parallelize(5 until 15).map(Bar.intToBar)
        }
      }

      val foo1InputOperator = ExternalInput
        .newInstance("foo1/part-*",
          new ExternalInputInfo.Basic(
            ClassDescription.of(classOf[Foo]),
            "foos1",
            ClassDescription.of(classOf[Foo]),
            ExternalInputInfo.DataSize.UNKNOWN))

      val foo2InputOperator = ExternalInput
        .newInstance("foo2/part-*",
          new ExternalInputInfo.Basic(
            ClassDescription.of(classOf[Foo]),
            "foos2",
            ClassDescription.of(classOf[Foo]),
            ExternalInputInfo.DataSize.UNKNOWN))

      val barInputOperator = ExternalInput
        .newInstance("bar/part-*",
          new ExternalInputInfo.Basic(
            ClassDescription.of(classOf[Bar]),
            "bars",
            ClassDescription.of(classOf[Bar]),
            ExternalInputInfo.DataSize.UNKNOWN))

      val masterCheckOperator = OperatorExtractor
        .extract(classOf[MasterCheck], classOf[Ops], "mastercheck")
        .input("foos", ClassDescription.of(classOf[Foo]),
          Groups.parse(Seq("id")),
          foo1InputOperator.getOperatorPort, foo2InputOperator.getOperatorPort)
        .input("bars", ClassDescription.of(classOf[Bar]),
          Groups.parse(Seq("fooId"), Seq("+id")),
          barInputOperator.getOperatorPort)
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

      compile(graph, 6, path, classpath)
      execute(classpath, threshold, parallelism)

      spark { implicit sc =>
        {
          val found = readResult[Bar]("found", path)
            .map { bar =>
              (bar.id.get, bar.bar.getAsString)
            }.collect.toSeq.sortBy(_._1)
          assert(found.size === 5)
          assert(found === (5 until 10).map(i => (10 + i, s"bar${10 + i}")))
        }
        {
          val missed = readResult[Bar]("missed", path)
            .map { bar =>
              (bar.id.get, bar.fooId.get, bar.bar.getAsString)
            }.collect.toSeq.sortBy(_._1)
          assert(missed.size === 5)
          assert(missed === (10 until 15).map(i => (10 + i, i, s"bar${10 + i}")))
        }
      }
    }

    it should s"compile Spark client with broadcast MasterCheck: ${configuration}" in {
      val (path, classpath) = createTempDirs()

      spark { implicit sc =>
        prepareData("foo", path) {
          sc.parallelize(0 until 10).map(Foo.intToFoo)
        }
        prepareData("bar", path) {
          sc.parallelize(5 until 15).map(Bar.intToBar)
        }
      }

      val fooInputOperator = ExternalInput
        .newInstance("foo/part-*",
          new ExternalInputInfo.Basic(
            ClassDescription.of(classOf[Foo]),
            "foos1",
            ClassDescription.of(classOf[Foo]),
            ExternalInputInfo.DataSize.TINY))

      val barInputOperator = ExternalInput
        .newInstance("bar/part-*",
          new ExternalInputInfo.Basic(
            ClassDescription.of(classOf[Bar]),
            "bars",
            ClassDescription.of(classOf[Bar]),
            ExternalInputInfo.DataSize.UNKNOWN))

      val masterCheckOperator = OperatorExtractor
        .extract(classOf[MasterCheck], classOf[Ops], "mastercheck")
        .input("foos", ClassDescription.of(classOf[Foo]),
          Groups.parse(Seq("id")),
          fooInputOperator.getOperatorPort)
        .input("bars", ClassDescription.of(classOf[Bar]),
          Groups.parse(Seq("fooId"), Seq("+id")),
          barInputOperator.getOperatorPort)
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

      compile(graph, 4, path, classpath)
      execute(classpath, threshold, parallelism)

      spark { implicit sc =>
        {
          val found = readResult[Bar]("found", path)
            .map { bar =>
              (bar.id.get, bar.bar.getAsString)
            }.collect.toSeq.sortBy(_._1)
          assert(found.size === 5)
          assert(found === (5 until 10).map(i => (10 + i, s"bar${10 + i}")))
        }
        {
          val missed = readResult[Bar]("missed", path)
            .map { bar =>
              (bar.id.get, bar.fooId.get, bar.bar.getAsString)
            }.collect.toSeq.sortBy(_._1)
          assert(missed.size === 5)
          assert(missed === (10 until 15).map(i => (10 + i, i, s"bar${10 + i}")))
        }
      }
    }

    it should s"compile Spark client with MasterJoin: ${configuration}" in {
      val (path, classpath) = createTempDirs()

      spark { implicit sc =>
        prepareData("foo1", path) {
          sc.parallelize(0 until 5).map(Foo.intToFoo)
        }
        prepareData("foo2", path) {
          sc.parallelize(5 until 10).map(Foo.intToFoo)
        }
        prepareData("bar", path) {
          sc.parallelize(5 until 15).map(Bar.intToBar)
        }
      }

      val foo1InputOperator = ExternalInput
        .newInstance("foo1/part-*",
          new ExternalInputInfo.Basic(
            ClassDescription.of(classOf[Foo]),
            "foos1",
            ClassDescription.of(classOf[Foo]),
            ExternalInputInfo.DataSize.UNKNOWN))

      val foo2InputOperator = ExternalInput
        .newInstance("foo2/part-*",
          new ExternalInputInfo.Basic(
            ClassDescription.of(classOf[Foo]),
            "foos2",
            ClassDescription.of(classOf[Foo]),
            ExternalInputInfo.DataSize.UNKNOWN))

      val barInputOperator = ExternalInput
        .newInstance("bar/part-*",
          new ExternalInputInfo.Basic(
            ClassDescription.of(classOf[Bar]),
            "bars",
            ClassDescription.of(classOf[Bar]),
            ExternalInputInfo.DataSize.UNKNOWN))

      val masterCheckOperator = OperatorExtractor
        .extract(classOf[MasterJoin], classOf[Ops], "masterjoin")
        .input("foos", ClassDescription.of(classOf[Foo]),
          Groups.parse(Seq("id")),
          foo1InputOperator.getOperatorPort, foo2InputOperator.getOperatorPort)
        .input("bars", ClassDescription.of(classOf[Bar]),
          Groups.parse(Seq("fooId"), Seq("+id")),
          barInputOperator.getOperatorPort)
        .output("joined", ClassDescription.of(classOf[FooBar]))
        .output("missed", ClassDescription.of(classOf[Bar]))
        .build()

      val joinedOutputOperator = ExternalOutput
        .newInstance("joined", masterCheckOperator.findOutput("joined"))

      val missedOutputOperator = ExternalOutput
        .newInstance("missed", masterCheckOperator.findOutput("missed"))

      val graph = new OperatorGraph(Seq(
        foo1InputOperator, foo2InputOperator, barInputOperator,
        masterCheckOperator,
        joinedOutputOperator, missedOutputOperator))

      compile(graph, 6, path, classpath)
      execute(classpath, threshold, parallelism)

      spark { implicit sc =>
        {
          val found = readResult[FooBar]("joined", path)
            .map { foobar =>
              (foobar.id.get, foobar.foo.getAsString, foobar.bar.getAsString)
            }.collect.toSeq.sortBy(_._1)
          assert(found.size === 5)
          assert(found === (5 until 10).map(i => (i, s"foo${i}", s"bar${10 + i}")))
        }
        {
          val missed = readResult[Bar]("missed", path)
            .map { bar =>
              (bar.id.get, bar.fooId.get, bar.bar.getAsString)
            }.collect.toSeq.sortBy(_._1)
          assert(missed.size === 5)
          assert(missed === (10 until 15).map(i => (10 + i, i, s"bar${10 + i}")))
        }
      }
    }

    it should s"compile Spark client with broadcast MasterJoin: ${configuration}" in {
      val (path, classpath) = createTempDirs()

      spark { implicit sc =>
        prepareData("foo", path) {
          sc.parallelize(0 until 10).map(Foo.intToFoo)
        }
        prepareData("bar", path) {
          sc.parallelize(5 until 15).map(Bar.intToBar)
        }
      }

      val fooInputOperator = ExternalInput
        .newInstance("foo/part-*",
          new ExternalInputInfo.Basic(
            ClassDescription.of(classOf[Foo]),
            "foos1",
            ClassDescription.of(classOf[Foo]),
            ExternalInputInfo.DataSize.TINY))

      val barInputOperator = ExternalInput
        .newInstance("bar/part-*",
          new ExternalInputInfo.Basic(
            ClassDescription.of(classOf[Bar]),
            "bars",
            ClassDescription.of(classOf[Bar]),
            ExternalInputInfo.DataSize.UNKNOWN))

      val masterCheckOperator = OperatorExtractor
        .extract(classOf[MasterJoin], classOf[Ops], "masterjoin")
        .input("foos", ClassDescription.of(classOf[Foo]),
          Groups.parse(Seq("id")),
          fooInputOperator.getOperatorPort)
        .input("bars", ClassDescription.of(classOf[Bar]),
          Groups.parse(Seq("fooId"), Seq("+id")),
          barInputOperator.getOperatorPort)
        .output("joined", ClassDescription.of(classOf[FooBar]))
        .output("missed", ClassDescription.of(classOf[Bar]))
        .build()

      val foundOutputOperator = ExternalOutput
        .newInstance("joined", masterCheckOperator.findOutput("joined"))

      val missedOutputOperator = ExternalOutput
        .newInstance("missed", masterCheckOperator.findOutput("missed"))

      val graph = new OperatorGraph(Seq(
        fooInputOperator, barInputOperator,
        masterCheckOperator,
        foundOutputOperator, missedOutputOperator))

      compile(graph, 4, path, classpath)
      execute(classpath, threshold, parallelism)

      spark { implicit sc =>
        {
          val found = readResult[FooBar]("joined", path)
            .map { foobar =>
              (foobar.id.get, foobar.foo.getAsString, foobar.bar.getAsString)
            }.collect.toSeq.sortBy(_._1)
          assert(found.size === 5)
          assert(found === (5 until 10).map(i => (i, s"foo${i}", s"bar${i + 10}")))
        }
        {
          val missed = readResult[Bar]("missed", path)
            .map { bar =>
              (bar.id.get, bar.fooId.get, bar.bar.getAsString)
            }.collect.toSeq.sortBy(_._1)
          assert(missed.size === 5)
          assert(missed === (10 until 15).map(i => (10 + i, i, s"bar${10 + i}")))
        }
      }
    }

    it should s"compile Spark client with broadcast self MasterCheck: ${configuration}" in {
      val (path, classpath) = createTempDirs()

      spark { implicit sc =>
        prepareData("foo", path) {
          sc.parallelize(0 until 10).map(Foo.intToFoo)
        }
      }

      val fooInputOperator = ExternalInput
        .newInstance("foo/part-*",
          new ExternalInputInfo.Basic(
            ClassDescription.of(classOf[Foo]),
            "foos1",
            ClassDescription.of(classOf[Foo]),
            ExternalInputInfo.DataSize.TINY))

      val masterCheckOperator = OperatorExtractor
        .extract(classOf[MasterCheck], classOf[Ops], "mastercheck")
        .input("fooms", ClassDescription.of(classOf[Foo]),
          Groups.parse(Seq("id")),
          fooInputOperator.getOperatorPort)
        .input("foots", ClassDescription.of(classOf[Foo]),
          Groups.parse(Seq("id")),
          fooInputOperator.getOperatorPort)
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

      compile(graph, 4, path, classpath)
      execute(classpath, threshold, parallelism)

      spark { implicit sc =>
        {
          val found = readResult[Foo]("found", path)
            .map { foo =>
              (foo.id.get, foo.foo.getAsString)
            }.collect.toSeq.sortBy(_._1)
          assert(found.size === 10)
          assert(found === (0 until 10).map(i => (i, s"foo${i}")))
        }
        {
          val missed = readResult[Foo]("missed", path)
            .map { foo =>
              (foo.id.get, foo.foo.getAsString)
            }.collect.toSeq.sortBy(_._1)
          assert(missed.size === 0)
        }
      }
    }

    it should s"compile Spark client with Fold: ${configuration}" in {
      val (path, classpath) = createTempDirs()

      spark { implicit sc =>
        prepareData("baz1", path) {
          sc.parallelize(0 until 50).map(Baz.intToBaz)
        }
        prepareData("baz2", path) {
          sc.parallelize(50 until 100).map(Baz.intToBaz)
        }
      }

      val baz1InputOperator = ExternalInput
        .newInstance("baz1/part-*",
          new ExternalInputInfo.Basic(
            ClassDescription.of(classOf[Baz]),
            "baz1",
            ClassDescription.of(classOf[Baz]),
            ExternalInputInfo.DataSize.UNKNOWN))

      val baz2InputOperator = ExternalInput
        .newInstance("baz2/part-*",
          new ExternalInputInfo.Basic(
            ClassDescription.of(classOf[Baz]),
            "baz2",
            ClassDescription.of(classOf[Baz]),
            ExternalInputInfo.DataSize.UNKNOWN))

      val foldOperator = OperatorExtractor
        .extract(classOf[Fold], classOf[Ops], "fold")
        .input("bazs", ClassDescription.of(classOf[Baz]),
          Groups.parse(Seq("id")),
          baz1InputOperator.getOperatorPort, baz2InputOperator.getOperatorPort)
        .output("result", ClassDescription.of(classOf[Baz]))
        .build()

      val resultOutputOperator = ExternalOutput
        .newInstance("result", foldOperator.findOutput("result"))

      val graph = new OperatorGraph(Seq(
        baz1InputOperator, baz2InputOperator,
        foldOperator,
        resultOutputOperator))

      compile(graph, 4, path, classpath)
      execute(classpath, threshold, parallelism)

      spark { implicit sc =>
        {
          val result = readResult[Baz]("result", path)
            .map { baz =>
              (baz.id.get, baz.n.get)
            }.collect.toSeq.sortBy(_._1)
          assert(result.size === 2)
          assert(result(0)._1 === 0)
          assert(result(0)._2 === (0 until 100 by 2).map(_ * 100).sum)
          assert(result(1)._1 === 1)
          assert(result(1)._2 === (1 until 100 by 2).map(_ * 100).sum)
        }
      }
    }

    it should s"compile Spark client with Fold with grouping is empty: ${configuration}" in {
      val (path, classpath) = createTempDirs()

      spark { implicit sc =>
        prepareData("baz1", path) {
          sc.parallelize(0 until 50).map(Baz.intToBaz)
        }
        prepareData("baz2", path) {
          sc.parallelize(50 until 100).map(Baz.intToBaz)
        }
      }

      val baz1InputOperator = ExternalInput
        .newInstance("baz1/part-*",
          new ExternalInputInfo.Basic(
            ClassDescription.of(classOf[Baz]),
            "baz1",
            ClassDescription.of(classOf[Baz]),
            ExternalInputInfo.DataSize.UNKNOWN))

      val baz2InputOperator = ExternalInput
        .newInstance("baz2/part-*",
          new ExternalInputInfo.Basic(
            ClassDescription.of(classOf[Baz]),
            "baz2",
            ClassDescription.of(classOf[Baz]),
            ExternalInputInfo.DataSize.UNKNOWN))

      val foldOperator = OperatorExtractor
        .extract(classOf[Fold], classOf[Ops], "fold")
        .input("bazs", ClassDescription.of(classOf[Baz]),
          Groups.parse(Seq.empty[String]),
          baz1InputOperator.getOperatorPort, baz2InputOperator.getOperatorPort)
        .output("result", ClassDescription.of(classOf[Baz]))
        .build()

      val resultOutputOperator = ExternalOutput
        .newInstance("result", foldOperator.findOutput("result"))

      val graph = new OperatorGraph(Seq(
        baz1InputOperator, baz2InputOperator,
        foldOperator,
        resultOutputOperator))

      compile(graph, 4, path, classpath)
      execute(classpath, threshold, parallelism)

      spark { implicit sc =>
        {
          val result = readResult[Baz]("result", path)
            .map { baz =>
              (baz.id.get, baz.n.get)
            }.collect.toSeq.sortBy(_._1)
          assert(result.size === 1)
          assert(result(0)._2 === (0 until 100).map(_ * 100).sum)
        }
      }
    }

    it should s"compile Spark client with Summarize: ${configuration}" in {
      val (path, classpath) = createTempDirs()

      spark { implicit sc =>
        prepareData("baz1", path) {
          sc.parallelize(0 until 500).map(Baz.intToBaz)
        }
        prepareData("baz2", path) {
          sc.parallelize(500 until 1000).map(Baz.intToBaz)
        }
      }

      val baz1InputOperator = ExternalInput
        .newInstance("baz1/part-*",
          new ExternalInputInfo.Basic(
            ClassDescription.of(classOf[Baz]),
            "baz1",
            ClassDescription.of(classOf[Baz]),
            ExternalInputInfo.DataSize.UNKNOWN))

      val baz2InputOperator = ExternalInput
        .newInstance("baz2/part-*",
          new ExternalInputInfo.Basic(
            ClassDescription.of(classOf[Baz]),
            "baz2",
            ClassDescription.of(classOf[Baz]),
            ExternalInputInfo.DataSize.UNKNOWN))

      val summarizeOperator = OperatorExtractor
        .extract(classOf[Summarize], classOf[Ops], "summarize")
        .input("bazs", ClassDescription.of(classOf[Baz]),
          Groups.parse(Seq("id")),
          baz1InputOperator.getOperatorPort, baz2InputOperator.getOperatorPort)
        .output("result", ClassDescription.of(classOf[SummarizedBaz]))
        .build()

      val resultOutputOperator = ExternalOutput
        .newInstance("result", summarizeOperator.findOutput("result"))

      val graph = new OperatorGraph(Seq(
        baz1InputOperator, baz2InputOperator,
        summarizeOperator,
        resultOutputOperator))

      compile(graph, 4, path, classpath)
      execute(classpath, threshold, parallelism)

      spark { implicit sc =>
        {
          val result = readResult[SummarizedBaz]("result", path)
            .map { baz =>
              (baz.id.get, baz.sum.get, baz.max.get, baz.min.get, baz.count.get)
            }.collect.toSeq.sortBy(_._1)
          assert(result.size === 2)
          assert(result(0)._1 === 0)
          assert(result(0)._2 === (0 until 1000 by 2).map(_ * 100).sum)
          assert(result(0)._3 === 99800)
          assert(result(0)._4 === 0)
          assert(result(0)._5 === 500)
          assert(result(1)._1 === 1)
          assert(result(1)._2 === (1 until 1000 by 2).map(_ * 100).sum)
          assert(result(1)._3 === 99900)
          assert(result(1)._4 === 100)
          assert(result(1)._5 === 500)
        }
      }
    }

    it should s"compile Spark client with Summarize with grouping is empty: ${configuration}" in {
      val (path, classpath) = createTempDirs()

      spark { implicit sc =>
        prepareData("baz1", path) {
          sc.parallelize(0 until 500).map(Baz.intToBaz)
        }
        prepareData("baz2", path) {
          sc.parallelize(500 until 1000).map(Baz.intToBaz)
        }
      }

      val baz1InputOperator = ExternalInput
        .newInstance("baz1/part-*",
          new ExternalInputInfo.Basic(
            ClassDescription.of(classOf[Baz]),
            "baz1",
            ClassDescription.of(classOf[Baz]),
            ExternalInputInfo.DataSize.UNKNOWN))

      val baz2InputOperator = ExternalInput
        .newInstance("baz2/part-*",
          new ExternalInputInfo.Basic(
            ClassDescription.of(classOf[Baz]),
            "baz2",
            ClassDescription.of(classOf[Baz]),
            ExternalInputInfo.DataSize.UNKNOWN))

      val summarizeOperator = OperatorExtractor
        .extract(classOf[Summarize], classOf[Ops], "summarize")
        .input("bazs", ClassDescription.of(classOf[Baz]),
          Groups.parse(Seq.empty[String]),
          baz1InputOperator.getOperatorPort, baz2InputOperator.getOperatorPort)
        .output("result", ClassDescription.of(classOf[SummarizedBaz]))
        .build()

      val resultOutputOperator = ExternalOutput
        .newInstance("result", summarizeOperator.findOutput("result"))

      val graph = new OperatorGraph(Seq(
        baz1InputOperator, baz2InputOperator,
        summarizeOperator,
        resultOutputOperator))

      compile(graph, 4, path, classpath)
      execute(classpath, threshold, parallelism)

      spark { implicit sc =>
        {
          val result = readResult[SummarizedBaz]("result", path)
            .map { baz =>
              (baz.id.get, baz.sum.get, baz.max.get, baz.min.get, baz.count.get)
            }.collect.toSeq.sortBy(_._1)
          assert(result.size === 1)
          assert(result(0)._2 === (0 until 1000).map(_ * 100).sum)
          assert(result(0)._3 === 99900)
          assert(result(0)._4 === 0)
          assert(result(0)._5 === 1000)
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

  def newJPContext(path: File, classpath: File): MockJobflowProcessorContext = {
    val jpContext = new MockJobflowProcessorContext(
      new CompilerOptions("buildid", path.getPath, Map.empty[String, String]),
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

  def newJobflow(graph: OperatorGraph): Jobflow = {
    new Jobflow("flowId", ClassDescription.of(classOf[SparkClientCompilerSpec]), graph)
  }

  def newCompiler(subplans: Int): SparkClientCompiler = {
    new SparkClientCompiler {

      override def preparePlan(jpContext: JPContext, source: Jobflow): Plan = {
        val plan = super.preparePlan(jpContext, source)
        assert(plan.getElements.size === subplans)
        plan
      }
    }
  }

  def compile(graph: OperatorGraph, subplans: Int, path: File, classpath: File): Unit = {
    val jpContext = newJPContext(path, classpath)
    val jobflow = newJobflow(graph)

    val compiler = newCompiler(subplans)
    compiler.process(jpContext, jobflow)
  }

  def execute(classpath: File, threshold: Option[Int], parallelism: Option[Int]): Unit = {
    val cl = Thread.currentThread.getContextClassLoader
    try {
      val classloader = new URLClassLoader(Array(classpath.toURI.toURL), cl)
      Thread.currentThread.setContextClassLoader(classloader)
      val cls = Class.forName("com.asakusafw.generated.spark.flowId.SparkClient", true, classloader)
        .asSubclass(classOf[SparkClient])
      val instance = cls.newInstance()

      val conf = new SparkConf()
      conf.setAppName("AsakusaSparkClient")
      conf.setMaster("local[8]")
      threshold.foreach(i => conf.set("spark.shuffle.sort.bypassMergeThreshold", i.toString))

      parallelism.foreach(para => conf.set(Props.Parallelism, para.toString))

      val stageInfo = new IterativeStageInfo(
        new StageInfo(
          sys.props("user.name"), "batchId", "flowId", null, "executionId", Map.empty[String, String]),
        IterativeExtensions.builder().build())

      instance.execute(conf, stageInfo)
    } finally {
      Thread.currentThread.setContextClassLoader(cl)
    }
  }
}

object SparkClientCompilerSpec {

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
}
