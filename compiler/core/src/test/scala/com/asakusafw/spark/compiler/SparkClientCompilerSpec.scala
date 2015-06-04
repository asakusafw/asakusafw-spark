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
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.rdd.RDD

import com.asakusafw.bridge.stage.StageInfo
import com.asakusafw.lang.compiler.api.CompilerOptions
import com.asakusafw.lang.compiler.api.JobflowProcessor.{ Context => JPContext }
import com.asakusafw.lang.compiler.api.testing.MockJobflowProcessorContext
import com.asakusafw.lang.compiler.common.Location
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

class SparkClientCompilerSpec extends FlatSpec with LoadClassSugar with TempDir {

  import SparkClientCompilerSpec._

  behavior of classOf[SparkClientCompiler].getSimpleName

  for {
    (master, threshold) <- Seq(("local[8]", None), ("local[8]", Some(0)))
  } {
    def execute(graph: OperatorGraph, subplans: Int, path: File, classpath: File): Unit = {

      val compiler = new SparkClientCompiler {

        override def preparePlan(jpContext: JPContext, source: Jobflow): Plan = {
          val plan = super.preparePlan(jpContext, source)
          assert(plan.getElements.size === subplans)
          plan
        }
      }

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
        conf.setMaster(master)
        threshold.foreach(i => conf.set("spark.shuffle.sort.bypassMergeThreshold", i.toString))

        val stageInfo = new StageInfo(
          sys.props("user.name"), "batchId", "flowId", null, "executionId", Map.empty[String, String])
        conf.setHadoopConf(Props.StageInfo, stageInfo.serialize)

        instance.execute(conf)
      } finally {
        Thread.currentThread.setContextClassLoader(cl)
      }
    }

    def createTempDirs(): (File, File) = {
      val tmpDir = createTempDirectory("test-").toFile
      val classpath = new File(tmpDir, "classes").getAbsoluteFile
      classpath.mkdirs()
      val path = new File(tmpDir, "tmp").getAbsoluteFile
      (path, classpath)
    }

    def prepareData[T: ClassTag](name: String, path: File)(rdd: RDD[T]): Unit = {
      val job = JobCompatibility.newJob(rdd.sparkContext.hadoopConfiguration)
      job.setOutputKeyClass(classOf[NullWritable])
      job.setOutputValueClass(classTag[T].runtimeClass)
      job.setOutputFormatClass(classOf[TemporaryOutputFormat[T]])
      TemporaryOutputFormat.setOutputPath(job, new Path(path.getPath, s"${MockJobflowProcessorContext.EXTERNAL_INPUT_BASE}${name}"))
      rdd.map((NullWritable.get, _)).saveAsNewAPIHadoopDataset(job.getConfiguration)
    }

    def readResult[T: ClassTag](sc: SparkContext, name: String, path: File): RDD[T] = {
      val job = JobCompatibility.newJob(sc.hadoopConfiguration)
      TemporaryInputFormat.setInputPaths(job, Seq(new Path(path.getPath, s"${name}/part-*")))
      sc.newAPIHadoopRDD(
        job.getConfiguration,
        classOf[TemporaryInputFormat[T]],
        classOf[NullWritable],
        classTag[T].runtimeClass.asInstanceOf[Class[T]]).map(_._2)
    }

    it should "compile Spark client from simple plan: " +
      s"[master=${master}${threshold.map(t => s",threshold=${t}").getOrElse("")}]" in {
        val (path, classpath) = createTempDirs()

        spark { sc =>
          prepareData("hoge", path) {
            sc.parallelize(0 until 100).map { i =>
              val hoge = new Hoge()
              hoge.id.modify(i)
              hoge.hoge.modify(s"hoge${i}")
              hoge
            }
          }
        }

        val inputOperator = ExternalInput
          .newInstance("hoge/part-*",
            new ExternalInputInfo.Basic(
              ClassDescription.of(classOf[Hoge]),
              "test",
              ClassDescription.of(classOf[Hoge]),
              ExternalInputInfo.DataSize.UNKNOWN))

        val outputOperator = ExternalOutput
          .newInstance("output", inputOperator.getOperatorPort)

        val graph = new OperatorGraph(Seq(inputOperator, outputOperator))
        execute(graph, 2, path, classpath)

        spark { sc =>
          val result = readResult[Hoge](sc, "output", path)
          assert(result.map(hoge => (hoge.id.get, hoge.hoge.getAsString)).collect.toSeq.sortBy(_._1) ===
            (0 until 100).map(i => (i, s"hoge${i}")))
        }
      }

    it should "compile Spark client with Extract: " +
      s"[master=${master}${threshold.map(t => s",threshold=${t}").getOrElse("")}]" in {
        val (path, classpath) = createTempDirs()

        spark { sc =>
          prepareData("hoge", path) {
            sc.parallelize(0 until 100).map { i =>
              val hoge = new Hoge()
              hoge.id.modify(i)
              hoge.hoge.modify(s"hoge${i}")
              hoge
            }
          }
        }

        val inputOperator = ExternalInput
          .newInstance("hoge/part-*",
            new ExternalInputInfo.Basic(
              ClassDescription.of(classOf[Hoge]),
              "test",
              ClassDescription.of(classOf[Hoge]),
              ExternalInputInfo.DataSize.UNKNOWN))

        val extractOperator = OperatorExtractor
          .extract(classOf[Extract], classOf[Ops], "extract")
          .input("hoge", ClassDescription.of(classOf[Hoge]), inputOperator.getOperatorPort)
          .output("evenResult", ClassDescription.of(classOf[Hoge]))
          .output("oddResult", ClassDescription.of(classOf[Hoge]))
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
        execute(graph, 3, path, classpath)

        spark { sc =>
          {
            val rdd = readResult[Hoge](sc, "even", path)
            assert(rdd.map(hoge => (hoge.id.get, hoge.hoge.getAsString)).collect.toSeq.sortBy(_._1) ===
              (0 until 100).filter(_ % 2 == 0).map(i => (i, s"hoge${i}")))
          }
          {
            val rdd = readResult[Hoge](sc, "odd", path)
            assert(rdd.map(hoge => (hoge.id.get, hoge.hoge.getAsString)).collect.toSeq.sortBy(_._1) ===
              (0 until 100).filterNot(_ % 2 == 0).map(i => (i, s"hoge${i}")))
          }
        }
      }

    it should "compile Spark client with CoGroup: " +
      s"[master=${master}${threshold.map(t => s",threshold=${t}").getOrElse("")}]" in {
        val (path, classpath) = createTempDirs()

        spark { sc =>
          prepareData("hoge1", path) {
            sc.parallelize(0 until 5).map { i =>
              val hoge = new Hoge()
              hoge.id.modify(i)
              hoge.hoge.modify(s"hoge${i}")
              hoge
            }
          }
          prepareData("hoge2", path) {
            sc.parallelize(5 until 10).map { i =>
              val hoge = new Hoge()
              hoge.id.modify(i)
              hoge.hoge.modify(s"hoge${i}")
              hoge
            }
          }
          prepareData("foo", path) {
            sc.parallelize(0 until 10).flatMap(i => (0 until i).map { j =>
              val foo = new Foo()
              foo.id.modify(10 + j)
              foo.hogeId.modify(i)
              foo.foo.modify(s"foo${10 + j}")
              foo
            })
          }
        }

        val hoge1InputOperator = ExternalInput
          .newInstance("hoge1/part-*",
            new ExternalInputInfo.Basic(
              ClassDescription.of(classOf[Hoge]),
              "hoges1",
              ClassDescription.of(classOf[Hoge]),
              ExternalInputInfo.DataSize.UNKNOWN))

        val hoge2InputOperator = ExternalInput
          .newInstance("hoge2/part-*",
            new ExternalInputInfo.Basic(
              ClassDescription.of(classOf[Hoge]),
              "hoges2",
              ClassDescription.of(classOf[Hoge]),
              ExternalInputInfo.DataSize.UNKNOWN))

        val fooInputOperator = ExternalInput
          .newInstance("foo/part-*",
            new ExternalInputInfo.Basic(
              ClassDescription.of(classOf[Foo]),
              "foos",
              ClassDescription.of(classOf[Foo]),
              ExternalInputInfo.DataSize.UNKNOWN))

        val cogroupOperator = OperatorExtractor
          .extract(classOf[CoGroup], classOf[Ops], "cogroup")
          .input("hoges", ClassDescription.of(classOf[Hoge]),
            Groups.parse(Seq("id")),
            hoge1InputOperator.getOperatorPort, hoge2InputOperator.getOperatorPort)
          .input("foos", ClassDescription.of(classOf[Foo]),
            Groups.parse(Seq("hogeId"), Seq("+id")),
            fooInputOperator.getOperatorPort)
          .output("hogeResult", ClassDescription.of(classOf[Hoge]))
          .output("fooResult", ClassDescription.of(classOf[Foo]))
          .output("hogeError", ClassDescription.of(classOf[Hoge]))
          .output("fooError", ClassDescription.of(classOf[Foo]))
          .build()

        val hogeResultOutputOperator = ExternalOutput
          .newInstance("hogeResult", cogroupOperator.findOutput("hogeResult"))

        val fooResultOutputOperator = ExternalOutput
          .newInstance("fooResult", cogroupOperator.findOutput("fooResult"))

        val hogeErrorOutputOperator = ExternalOutput
          .newInstance("hogeError", cogroupOperator.findOutput("hogeError"))

        val fooErrorOutputOperator = ExternalOutput
          .newInstance("fooError", cogroupOperator.findOutput("fooError"))

        val graph = new OperatorGraph(Seq(
          hoge1InputOperator, hoge2InputOperator, fooInputOperator,
          cogroupOperator,
          hogeResultOutputOperator, fooResultOutputOperator, hogeErrorOutputOperator, fooErrorOutputOperator))
        execute(graph, 8, path, classpath)

        spark { sc =>
          {
            val hogeResult = readResult[Hoge](sc, "hogeResult", path)
              .map(hoge => (hoge.id.get, hoge.hoge.getAsString)).collect.toSeq.sortBy(_._1)
            assert(hogeResult.size === 1)
            assert(hogeResult(0) === (1, "hoge1"))
          }
          {
            val fooResult = readResult[Foo](sc, "fooResult", path)
              .map(foo => (foo.id.get, foo.hogeId.get, foo.foo.getAsString)).collect.toSeq.sortBy(_._1)
            assert(fooResult.size === 1)
            assert(fooResult(0) === (10, 1, "foo10"))
          }
          {
            val hogeError = readResult[Hoge](sc, "hogeError", path)
              .map(hoge => (hoge.id.get, hoge.hoge.getAsString)).collect.toSeq.sortBy(_._1)
            assert(hogeError.size === 9)
            assert(hogeError(0) === (0, "hoge0"))
            for (i <- 2 until 10) {
              assert(hogeError(i - 1) === (i, s"hoge${i}"))
            }
          }
          {
            val fooError = readResult[Foo](sc, "fooError", path)
              .map(foo => (foo.id.get, foo.hogeId.get, foo.foo.getAsString)).collect.toSeq.sortBy(_._1)
              .sortBy(foo => (foo._2, foo._1))
            assert(fooError.size === 44)
            for {
              i <- 2 until 10
              j <- 0 until i
            } {
              assert(fooError((i * (i - 1)) / 2 + j - 1) === (10 + j, i, s"foo${10 + j}"))
            }
          }
        }
      }

    it should "compile Spark client with MasterCheck: " +
      s"[master=${master}${threshold.map(t => s",threshold=${t}").getOrElse("")}]" in {
        val (path, classpath) = createTempDirs()

        spark { sc =>
          prepareData("hoge1", path) {
            sc.parallelize(0 until 5).map { i =>
              val hoge = new Hoge()
              hoge.id.modify(i)
              hoge.hoge.modify(s"hoge${i}")
              hoge
            }
          }
          prepareData("hoge2", path) {
            sc.parallelize(5 until 10).map { i =>
              val hoge = new Hoge()
              hoge.id.modify(i)
              hoge.hoge.modify(s"hoge${i}")
              hoge
            }
          }
          prepareData("foo", path) {
            sc.parallelize(5 until 15).map { i =>
              val foo = new Foo()
              foo.id.modify(10 + i)
              foo.hogeId.modify(i)
              foo.foo.modify(s"foo${10 + i}")
              foo
            }
          }
        }

        val hoge1InputOperator = ExternalInput
          .newInstance("hoge1/part-*",
            new ExternalInputInfo.Basic(
              ClassDescription.of(classOf[Hoge]),
              "hoges1",
              ClassDescription.of(classOf[Hoge]),
              ExternalInputInfo.DataSize.UNKNOWN))

        val hoge2InputOperator = ExternalInput
          .newInstance("hoge2/part-*",
            new ExternalInputInfo.Basic(
              ClassDescription.of(classOf[Hoge]),
              "hoges2",
              ClassDescription.of(classOf[Hoge]),
              ExternalInputInfo.DataSize.UNKNOWN))

        val fooInputOperator = ExternalInput
          .newInstance("foo/part-*",
            new ExternalInputInfo.Basic(
              ClassDescription.of(classOf[Foo]),
              "foos",
              ClassDescription.of(classOf[Foo]),
              ExternalInputInfo.DataSize.UNKNOWN))

        val masterCheckOperator = OperatorExtractor
          .extract(classOf[MasterCheck], classOf[Ops], "mastercheck")
          .input("hoges", ClassDescription.of(classOf[Hoge]),
            Groups.parse(Seq("id")),
            hoge1InputOperator.getOperatorPort, hoge2InputOperator.getOperatorPort)
          .input("foos", ClassDescription.of(classOf[Foo]),
            Groups.parse(Seq("hogeId"), Seq("+id")),
            fooInputOperator.getOperatorPort)
          .output("found", ClassDescription.of(classOf[Foo]))
          .output("missed", ClassDescription.of(classOf[Foo]))
          .build()

        val foundOutputOperator = ExternalOutput
          .newInstance("found", masterCheckOperator.findOutput("found"))

        val missedOutputOperator = ExternalOutput
          .newInstance("missed", masterCheckOperator.findOutput("missed"))

        val graph = new OperatorGraph(Seq(
          hoge1InputOperator, hoge2InputOperator, fooInputOperator,
          masterCheckOperator,
          foundOutputOperator, missedOutputOperator))
        execute(graph, 6, path, classpath)

        spark { sc =>
          {
            val found = readResult[Foo](sc, "found", path)
              .map(foo => (foo.id.get, foo.foo.getAsString)).collect.toSeq.sortBy(_._1)
            assert(found.size === 5)
            assert(found === (5 until 10).map(i => (10 + i, s"foo${10 + i}")))
          }
          {
            val missed = readResult[Foo](sc, "missed", path)
              .map(foo => (foo.id.get, foo.hogeId.get, foo.foo.getAsString)).collect.toSeq.sortBy(_._1)
            assert(missed.size === 5)
            assert(missed === (10 until 15).map(i => (10 + i, i, s"foo${10 + i}")))
          }
        }
      }

    it should "compile Spark client with broadcast MasterCheck: " +
      s"[master=${master}${threshold.map(t => s",threshold=${t}").getOrElse("")}]" in {
        val (path, classpath) = createTempDirs()

        spark { sc =>
          prepareData("hoge", path) {
            sc.parallelize(0 until 10).map { i =>
              val hoge = new Hoge()
              hoge.id.modify(i)
              hoge.hoge.modify(s"hoge${i}")
              hoge
            }
          }
          prepareData("foo", path) {
            sc.parallelize(5 until 15).map { i =>
              val foo = new Foo()
              foo.id.modify(10 + i)
              foo.hogeId.modify(i)
              foo.foo.modify(s"foo${10 + i}")
              foo
            }
          }
        }

        val hogeInputOperator = ExternalInput
          .newInstance("hoge/part-*",
            new ExternalInputInfo.Basic(
              ClassDescription.of(classOf[Hoge]),
              "hoges1",
              ClassDescription.of(classOf[Hoge]),
              ExternalInputInfo.DataSize.TINY))

        val fooInputOperator = ExternalInput
          .newInstance("foo/part-*",
            new ExternalInputInfo.Basic(
              ClassDescription.of(classOf[Foo]),
              "foos",
              ClassDescription.of(classOf[Foo]),
              ExternalInputInfo.DataSize.UNKNOWN))

        val masterCheckOperator = OperatorExtractor
          .extract(classOf[MasterCheck], classOf[Ops], "mastercheck")
          .input("hoges", ClassDescription.of(classOf[Hoge]),
            Groups.parse(Seq("id")),
            hogeInputOperator.getOperatorPort)
          .input("foos", ClassDescription.of(classOf[Foo]),
            Groups.parse(Seq("hogeId"), Seq("+id")),
            fooInputOperator.getOperatorPort)
          .output("found", ClassDescription.of(classOf[Foo]))
          .output("missed", ClassDescription.of(classOf[Foo]))
          .build()

        val foundOutputOperator = ExternalOutput
          .newInstance("found", masterCheckOperator.findOutput("found"))

        val missedOutputOperator = ExternalOutput
          .newInstance("missed", masterCheckOperator.findOutput("missed"))

        val graph = new OperatorGraph(Seq(
          hogeInputOperator, fooInputOperator,
          masterCheckOperator,
          foundOutputOperator, missedOutputOperator))
        execute(graph, 4, path, classpath)

        spark { sc =>
          {
            val found = readResult[Foo](sc, "found", path)
              .map(foo => (foo.id.get, foo.foo.getAsString)).collect.toSeq.sortBy(_._1)
            assert(found.size === 5)
            assert(found === (5 until 10).map(i => (10 + i, s"foo${10 + i}")))
          }
          {
            val missed = readResult[Foo](sc, "missed", path)
              .map(foo => (foo.id.get, foo.hogeId.get, foo.foo.getAsString)).collect.toSeq.sortBy(_._1)
            assert(missed.size === 5)
            assert(missed === (10 until 15).map(i => (10 + i, i, s"foo${10 + i}")))
          }
        }
      }

    it should "compile Spark client with MasterJoin: " +
      s"[master=${master}${threshold.map(t => s",threshold=${t}").getOrElse("")}]" in {
        val (path, classpath) = createTempDirs()

        spark { sc =>
          prepareData("hoge1", path) {
            sc.parallelize(0 until 5).map { i =>
              val hoge = new Hoge()
              hoge.id.modify(i)
              hoge.hoge.modify(s"hoge${i}")
              hoge
            }
          }
          prepareData("hoge2", path) {
            sc.parallelize(5 until 10).map { i =>
              val hoge = new Hoge()
              hoge.id.modify(i)
              hoge.hoge.modify(s"hoge${i}")
              hoge
            }
          }
          prepareData("foo", path) {
            sc.parallelize(5 until 15).map { i =>
              val foo = new Foo()
              foo.id.modify(10 + i)
              foo.hogeId.modify(i)
              foo.foo.modify(s"foo${10 + i}")
              foo
            }
          }
        }

        val hoge1InputOperator = ExternalInput
          .newInstance("hoge1/part-*",
            new ExternalInputInfo.Basic(
              ClassDescription.of(classOf[Hoge]),
              "hoges1",
              ClassDescription.of(classOf[Hoge]),
              ExternalInputInfo.DataSize.UNKNOWN))

        val hoge2InputOperator = ExternalInput
          .newInstance("hoge2/part-*",
            new ExternalInputInfo.Basic(
              ClassDescription.of(classOf[Hoge]),
              "hoges2",
              ClassDescription.of(classOf[Hoge]),
              ExternalInputInfo.DataSize.UNKNOWN))

        val fooInputOperator = ExternalInput
          .newInstance("foo/part-*",
            new ExternalInputInfo.Basic(
              ClassDescription.of(classOf[Foo]),
              "foos",
              ClassDescription.of(classOf[Foo]),
              ExternalInputInfo.DataSize.UNKNOWN))

        val masterCheckOperator = OperatorExtractor
          .extract(classOf[MasterJoin], classOf[Ops], "masterjoin")
          .input("hoges", ClassDescription.of(classOf[Hoge]),
            Groups.parse(Seq("id")),
            hoge1InputOperator.getOperatorPort, hoge2InputOperator.getOperatorPort)
          .input("foos", ClassDescription.of(classOf[Foo]),
            Groups.parse(Seq("hogeId"), Seq("+id")),
            fooInputOperator.getOperatorPort)
          .output("joined", ClassDescription.of(classOf[HogeFoo]))
          .output("missed", ClassDescription.of(classOf[Foo]))
          .build()

        val joinedOutputOperator = ExternalOutput
          .newInstance("joined", masterCheckOperator.findOutput("joined"))

        val missedOutputOperator = ExternalOutput
          .newInstance("missed", masterCheckOperator.findOutput("missed"))

        val graph = new OperatorGraph(Seq(
          hoge1InputOperator, hoge2InputOperator, fooInputOperator,
          masterCheckOperator,
          joinedOutputOperator, missedOutputOperator))
        execute(graph, 6, path, classpath)

        spark { sc =>
          {
            val found = readResult[HogeFoo](sc, "joined", path)
              .map(hogefoo => (hogefoo.id.get, hogefoo.hoge.getAsString, hogefoo.foo.getAsString)).collect.toSeq.sortBy(_._1)
            assert(found.size === 5)
            assert(found === (5 until 10).map(i => (i, s"hoge${i}", s"foo${10 + i}")))
          }
          {
            val missed = readResult[Foo](sc, "missed", path)
              .map(foo => (foo.id.get, foo.hogeId.get, foo.foo.getAsString)).collect.toSeq.sortBy(_._1)
            assert(missed.size === 5)
            assert(missed === (10 until 15).map(i => (10 + i, i, s"foo${10 + i}")))
          }
        }
      }

    it should "compile Spark client with broadcast MasterJoin: " +
      s"[master=${master}${threshold.map(t => s",threshold=${t}").getOrElse("")}]" in {
        val (path, classpath) = createTempDirs()

        spark { sc =>
          prepareData("hoge", path) {
            sc.parallelize(0 until 10).map { i =>
              val hoge = new Hoge()
              hoge.id.modify(i)
              hoge.hoge.modify(s"hoge${i}")
              hoge
            }
          }
          prepareData("foo", path) {
            sc.parallelize(5 until 15).map { i =>
              val foo = new Foo()
              foo.id.modify(10 + i)
              foo.hogeId.modify(i)
              foo.foo.modify(s"foo${10 + i}")
              foo
            }
          }
        }

        val hogeInputOperator = ExternalInput
          .newInstance("hoge/part-*",
            new ExternalInputInfo.Basic(
              ClassDescription.of(classOf[Hoge]),
              "hoges1",
              ClassDescription.of(classOf[Hoge]),
              ExternalInputInfo.DataSize.TINY))

        val fooInputOperator = ExternalInput
          .newInstance("foo/part-*",
            new ExternalInputInfo.Basic(
              ClassDescription.of(classOf[Foo]),
              "foos",
              ClassDescription.of(classOf[Foo]),
              ExternalInputInfo.DataSize.UNKNOWN))

        val masterCheckOperator = OperatorExtractor
          .extract(classOf[MasterJoin], classOf[Ops], "masterjoin")
          .input("hoges", ClassDescription.of(classOf[Hoge]),
            Groups.parse(Seq("id")),
            hogeInputOperator.getOperatorPort)
          .input("foos", ClassDescription.of(classOf[Foo]),
            Groups.parse(Seq("hogeId"), Seq("+id")),
            fooInputOperator.getOperatorPort)
          .output("joined", ClassDescription.of(classOf[HogeFoo]))
          .output("missed", ClassDescription.of(classOf[Foo]))
          .build()

        val foundOutputOperator = ExternalOutput
          .newInstance("joined", masterCheckOperator.findOutput("joined"))

        val missedOutputOperator = ExternalOutput
          .newInstance("missed", masterCheckOperator.findOutput("missed"))

        val graph = new OperatorGraph(Seq(
          hogeInputOperator, fooInputOperator,
          masterCheckOperator,
          foundOutputOperator, missedOutputOperator))
        execute(graph, 4, path, classpath)

        spark { sc =>
          {
            val found = readResult[HogeFoo](sc, "joined", path)
              .map(hogefoo => (hogefoo.id.get, hogefoo.hoge.getAsString, hogefoo.foo.getAsString)).collect.toSeq.sortBy(_._1)
            assert(found.size === 5)
            assert(found === (5 until 10).map(i => (i, s"hoge${i}", s"foo${i + 10}")))
          }
          {
            val missed = readResult[Foo](sc, "missed", path)
              .map(foo => (foo.id.get, foo.hogeId.get, foo.foo.getAsString)).collect.toSeq.sortBy(_._1)
            assert(missed.size === 5)
            assert(missed === (10 until 15).map(i => (10 + i, i, s"foo${10 + i}")))
          }
        }
      }

    it should "compile Spark client with broadcast self MasterCheck: " +
      s"[master=${master}${threshold.map(t => s",threshold=${t}").getOrElse("")}]" in {
        val (path, classpath) = createTempDirs()

        spark { sc =>
          prepareData("hoge", path) {
            sc.parallelize(0 until 10).map { i =>
              val hoge = new Hoge()
              hoge.id.modify(i)
              hoge.hoge.modify(s"hoge${i}")
              hoge
            }
          }
        }

        val hogeInputOperator = ExternalInput
          .newInstance("hoge/part-*",
            new ExternalInputInfo.Basic(
              ClassDescription.of(classOf[Hoge]),
              "hoges1",
              ClassDescription.of(classOf[Hoge]),
              ExternalInputInfo.DataSize.TINY))

        val masterCheckOperator = OperatorExtractor
          .extract(classOf[MasterCheck], classOf[Ops], "mastercheck")
          .input("hogems", ClassDescription.of(classOf[Hoge]),
            Groups.parse(Seq("id")),
            hogeInputOperator.getOperatorPort)
          .input("hogets", ClassDescription.of(classOf[Hoge]),
            Groups.parse(Seq("id")),
            hogeInputOperator.getOperatorPort)
          .output("found", ClassDescription.of(classOf[Hoge]))
          .output("missed", ClassDescription.of(classOf[Hoge]))
          .build()

        val foundOutputOperator = ExternalOutput
          .newInstance("found", masterCheckOperator.findOutput("found"))

        val missedOutputOperator = ExternalOutput
          .newInstance("missed", masterCheckOperator.findOutput("missed"))

        val graph = new OperatorGraph(Seq(
          hogeInputOperator,
          masterCheckOperator,
          foundOutputOperator, missedOutputOperator))
        execute(graph, 4, path, classpath)

        spark { sc =>
          {
            val found = readResult[Hoge](sc, "found", path)
              .map(hoge => (hoge.id.get, hoge.hoge.getAsString)).collect.toSeq.sortBy(_._1)
            assert(found.size === 10)
            assert(found === (0 until 10).map(i => (i, s"hoge${i}")))
          }
          {
            val missed = readResult[Hoge](sc, "missed", path)
              .map(hoge => (hoge.id.get, hoge.hoge.getAsString)).collect.toSeq.sortBy(_._1)
            assert(missed.size === 0)
          }
        }
      }

    it should "compile Spark client with Fold: " +
      s"[master=${master}${threshold.map(t => s",threshold=${t}").getOrElse("")}]" in {
        val (path, classpath) = createTempDirs()

        spark { sc =>
          prepareData("baa1", path) {
            sc.parallelize(0 until 50).map { i =>
              val baa = new Baa()
              baa.id.modify(i % 2)
              baa.price.modify(100 * i)
              baa
            }
          }
          prepareData("baa2", path) {
            sc.parallelize(50 until 100).map { i =>
              val baa = new Baa()
              baa.id.modify(i % 2)
              baa.price.modify(100 * i)
              baa
            }
          }
        }

        val baa1InputOperator = ExternalInput
          .newInstance("baa1/part-*",
            new ExternalInputInfo.Basic(
              ClassDescription.of(classOf[Baa]),
              "baa1",
              ClassDescription.of(classOf[Baa]),
              ExternalInputInfo.DataSize.UNKNOWN))

        val baa2InputOperator = ExternalInput
          .newInstance("baa2/part-*",
            new ExternalInputInfo.Basic(
              ClassDescription.of(classOf[Baa]),
              "baa2",
              ClassDescription.of(classOf[Baa]),
              ExternalInputInfo.DataSize.UNKNOWN))

        val foldOperator = OperatorExtractor
          .extract(classOf[Fold], classOf[Ops], "fold")
          .input("baas", ClassDescription.of(classOf[Baa]),
            Groups.parse(Seq("id")),
            baa1InputOperator.getOperatorPort, baa2InputOperator.getOperatorPort)
          .output("result", ClassDescription.of(classOf[Baa]))
          .build()

        val resultOutputOperator = ExternalOutput
          .newInstance("result", foldOperator.findOutput("result"))

        val graph = new OperatorGraph(Seq(
          baa1InputOperator, baa2InputOperator,
          foldOperator,
          resultOutputOperator))
        execute(graph, 4, path, classpath)

        spark { sc =>
          {
            val result = readResult[Baa](sc, "result", path)
              .map { baa => (baa.id.get, baa.price.get) }.collect.toSeq.sortBy(_._1)
            assert(result.size === 2)
            assert(result(0)._1 === 0)
            assert(result(0)._2 === (0 until 100 by 2).map(_ * 100).sum)
            assert(result(1)._1 === 1)
            assert(result(1)._2 === (1 until 100 by 2).map(_ * 100).sum)
          }
        }
      }

    it should "compile Spark client with Summarize: " +
      s"[master=${master}${threshold.map(t => s",threshold=${t}").getOrElse("")}]" in {
        val (path, classpath) = createTempDirs()

        spark { sc =>
          prepareData("baa1", path) {
            sc.parallelize(0 until 500).map { i =>
              val baa = new Baa()
              baa.id.modify(i % 2)
              baa.price.modify(100 * i)
              baa
            }
          }
          prepareData("baa2", path) {
            sc.parallelize(500 until 1000).map { i =>
              val baa = new Baa()
              baa.id.modify(i % 2)
              baa.price.modify(100 * i)
              baa
            }
          }
        }

        val baa1InputOperator = ExternalInput
          .newInstance("baa1/part-*",
            new ExternalInputInfo.Basic(
              ClassDescription.of(classOf[Baa]),
              "baa1",
              ClassDescription.of(classOf[Baa]),
              ExternalInputInfo.DataSize.UNKNOWN))

        val baa2InputOperator = ExternalInput
          .newInstance("baa2/part-*",
            new ExternalInputInfo.Basic(
              ClassDescription.of(classOf[Baa]),
              "baa2",
              ClassDescription.of(classOf[Baa]),
              ExternalInputInfo.DataSize.UNKNOWN))

        val summarizeOperator = OperatorExtractor
          .extract(classOf[Summarize], classOf[Ops], "summarize")
          .input("baas", ClassDescription.of(classOf[Baa]),
            Groups.parse(Seq("id")),
            baa1InputOperator.getOperatorPort, baa2InputOperator.getOperatorPort)
          .output("result", ClassDescription.of(classOf[SummarizedBaa]))
          .build()

        val resultOutputOperator = ExternalOutput
          .newInstance("result", summarizeOperator.findOutput("result"))

        val graph = new OperatorGraph(Seq(
          baa1InputOperator, baa2InputOperator,
          summarizeOperator,
          resultOutputOperator))
        execute(graph, 4, path, classpath)

        spark { sc =>
          {
            val result = readResult[SummarizedBaa](sc, "result", path)
              .map { baa =>
                (baa.id.get, baa.priceSum.get, baa.priceMax.get, baa.priceMin.get, baa.count.get)
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
    val hoge = new StringOption()

    override def reset(): Unit = {
      id.setNull()
      hoge.setNull()
    }
    override def copyFrom(other: Hoge): Unit = {
      id.copyFrom(other.id)
      hoge.copyFrom(other.hoge)
    }
    override def readFields(in: DataInput): Unit = {
      id.readFields(in)
      hoge.readFields(in)
    }
    override def write(out: DataOutput): Unit = {
      id.write(out)
      hoge.write(out)
    }

    def getIdOption: IntOption = id
    def getHogeOption: StringOption = hoge
  }

  class Foo extends DataModel[Foo] with Writable {

    val id = new IntOption()
    val hogeId = new IntOption()
    val foo = new StringOption()

    override def reset(): Unit = {
      id.setNull()
      hogeId.setNull()
      foo.setNull()
    }
    override def copyFrom(other: Foo): Unit = {
      id.copyFrom(other.id)
      hogeId.copyFrom(other.hogeId)
      foo.copyFrom(other.foo)
    }
    override def readFields(in: DataInput): Unit = {
      id.readFields(in)
      hogeId.readFields(in)
      foo.readFields(in)
    }
    override def write(out: DataOutput): Unit = {
      id.write(out)
      hogeId.write(out)
      foo.write(out)
    }

    def getIdOption: IntOption = id
    def getHogeIdOption: IntOption = hogeId
    def getFooOption: StringOption = foo
  }

  @Joined(terms = Array(
    new Joined.Term(source = classOf[Hoge], shuffle = new Key(group = Array("id")), mappings = Array(
      new Joined.Mapping(source = "id", destination = "id"),
      new Joined.Mapping(source = "hoge", destination = "hoge"))),
    new Joined.Term(source = classOf[Foo], shuffle = new Key(group = Array("hogeId")), mappings = Array(
      new Joined.Mapping(source = "hogeId", destination = "id"),
      new Joined.Mapping(source = "foo", destination = "foo")))))
  class HogeFoo extends DataModel[HogeFoo] with Writable {

    val id = new IntOption()
    val hoge = new StringOption()
    val foo = new StringOption()

    override def reset(): Unit = {
      id.setNull()
      hoge.setNull()
      foo.setNull()
    }
    override def copyFrom(other: HogeFoo): Unit = {
      id.copyFrom(other.id)
      hoge.copyFrom(other.hoge)
      foo.copyFrom(other.foo)
    }
    override def readFields(in: DataInput): Unit = {
      id.readFields(in)
      hoge.readFields(in)
      foo.readFields(in)
    }
    override def write(out: DataOutput): Unit = {
      id.write(out)
      hoge.write(out)
      foo.write(out)
    }

    def getIdOption: IntOption = id
    def getHogeOption: StringOption = hoge
    def getFooOption: StringOption = foo
  }

  class Baa extends DataModel[Baa] with Writable {

    val id = new IntOption()
    val price = new IntOption()

    override def reset(): Unit = {
      id.setNull()
      price.setNull()
    }
    override def copyFrom(other: Baa): Unit = {
      id.copyFrom(other.id)
      price.copyFrom(other.price)
    }
    override def readFields(in: DataInput): Unit = {
      id.readFields(in)
      price.readFields(in)
    }
    override def write(out: DataOutput): Unit = {
      id.write(out)
      price.write(out)
    }

    def getIdOption: IntOption = id
    def getPriceOption: IntOption = price
  }

  @Summarized(term = new Summarized.Term(
    source = classOf[Baa],
    shuffle = new Key(group = Array("id")),
    foldings = Array(
      new Summarized.Folding(source = "id", destination = "id", aggregator = Summarized.Aggregator.ANY),
      new Summarized.Folding(source = "price", destination = "priceSum", aggregator = Summarized.Aggregator.SUM),
      new Summarized.Folding(source = "price", destination = "priceMax", aggregator = Summarized.Aggregator.MAX),
      new Summarized.Folding(source = "price", destination = "priceMin", aggregator = Summarized.Aggregator.MIN),
      new Summarized.Folding(source = "id", destination = "count", aggregator = Summarized.Aggregator.COUNT))))
  class SummarizedBaa extends DataModel[SummarizedBaa] with Writable {

    val id = new IntOption()
    val priceSum = new LongOption()
    val priceMax = new IntOption()
    val priceMin = new IntOption()
    val count = new LongOption()

    override def reset(): Unit = {
      id.setNull()
      priceSum.setNull()
      priceMax.setNull()
      priceMin.setNull()
      count.setNull()
    }
    override def copyFrom(other: SummarizedBaa): Unit = {
      id.copyFrom(other.id)
      priceSum.copyFrom(other.priceSum)
      priceMax.copyFrom(other.priceMax)
      priceMin.copyFrom(other.priceMin)
      count.copyFrom(other.count)
    }
    override def readFields(in: DataInput): Unit = {
      id.readFields(in)
      priceSum.readFields(in)
      priceMax.readFields(in)
      priceMin.readFields(in)
      count.readFields(in)
    }
    override def write(out: DataOutput): Unit = {
      id.write(out)
      priceSum.write(out)
      priceMax.write(out)
      priceMin.write(out)
      count.write(out)
    }

    def getIdOption: IntOption = id
    def getPriceSumOption: LongOption = priceSum
    def getPriceMaxOption: IntOption = priceMax
    def getPriceMinOption: IntOption = priceMin
    def getCountOption: LongOption = count
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

    @MasterCheck
    def mastercheck(hoge: Hoge, foo: Foo): Boolean = ???

    @MasterJoin
    def masterjoin(hoge: Hoge, foo: Foo): Baa = ???

    @Fold(partialAggregation = PartialAggregation.PARTIAL)
    def fold(acc: Baa, each: Baa): Unit = {
      acc.price.add(each.price)
    }

    @Summarize
    def summarize(value: Baa): SummarizedBaa = ???
  }
}
