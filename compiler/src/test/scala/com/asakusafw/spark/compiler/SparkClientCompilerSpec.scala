/*
 * Copyright 2011-2017 Asakusa Framework Team.
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
import java.io.{ DataInput, DataOutput, File }
import java.net.URLClassLoader

import com.asakusafw.bridge.hadoop.temporary.{ TemporaryFileInputFormat, TemporaryFileOutputFormat }

import scala.collection.JavaConversions._
import scala.reflect.{ classTag, ClassTag }
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{ NullWritable, Writable }
import org.apache.hadoop.mapreduce.{ Job => MRJob }
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.rdd.RDD
import com.asakusafw.bridge.stage.StageInfo
import com.asakusafw.iterative.common.IterativeExtensions
import com.asakusafw.iterative.launch.IterativeStageInfo
import com.asakusafw.lang.compiler.api.CompilerOptions
import com.asakusafw.lang.compiler.api.JobflowProcessor.{ Context => JPContext }
import com.asakusafw.lang.compiler.api.testing.MockJobflowProcessorContext
import com.asakusafw.lang.compiler.common.Location
import com.asakusafw.lang.compiler.inspection.{ AbstractInspectionExtension, InspectionExtension }
import com.asakusafw.lang.compiler.model.description.ClassDescription
import com.asakusafw.lang.compiler.model.graph._
import com.asakusafw.lang.compiler.model.info.ExternalInputInfo
import com.asakusafw.lang.compiler.planning.Plan
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.runtime.value._
import com.asakusafw.spark.runtime._
import com.asakusafw.spark.tools.asm._
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

@RunWith(classOf[JUnitRunner])
class SparkClientCompilerSpecTest extends SparkClientCompilerSpec

class SparkClientCompilerSpec extends FlatSpec with LoadClassSugar with TempDirForEach {

  import SparkClientCompilerSpec._

  behavior of classOf[SparkClientCompiler].getSimpleName

  def createTempDirs(): (File, File) = {
    val tmpDir = createTempDirectoryForEach("test-").toFile
    val classpath = new File(tmpDir, "classes").getAbsoluteFile
    classpath.mkdirs()
    val path = new File(tmpDir, "tmp").getAbsoluteFile
    (path, classpath)
  }

  def prepareData[T: ClassTag](
    name: String, path: File)(
      rdd: RDD[T])(
        implicit sc: SparkContext): Unit = {
    val job = MRJob.getInstance(sc.hadoopConfiguration)
    job.setOutputKeyClass(classOf[NullWritable])
    job.setOutputValueClass(classTag[T].runtimeClass)
    job.setOutputFormatClass(classOf[TemporaryFileOutputFormat[_]])
    FileOutputFormat.setOutputPath(
      job,
      new Path(path.getPath, s"${MockJobflowProcessorContext.EXTERNAL_INPUT_BASE}${name}"))
    rdd.map((NullWritable.get, _)).saveAsNewAPIHadoopDataset(job.getConfiguration)
  }

  def readResult[T: ClassTag](
    name: String, path: File)(
      implicit sc: SparkContext): RDD[T] = {
    val job = MRJob.getInstance(sc.hadoopConfiguration)
    FileInputFormat.setInputPaths(job, new Path(path.getPath, s"${name}/-/part-*"))
    sc.newAPIHadoopRDD(
      job.getConfiguration,
      classOf[TemporaryFileInputFormat[T]],
      classOf[NullWritable],
      classTag[T].runtimeClass.asInstanceOf[Class[T]]).map(_._2)
  }

  it should "compile Spark client from simple plan" in {
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
    executeJob(classpath)

    spark { implicit sc =>
      val result = readResult[Foo]("output", path)
        .map { foo =>
          (foo.id.get, foo.foo.getAsString)
        }.collect.toSeq.sortBy(_._1)
      assert(result === (0 until 100).map(i => (i, s"foo${i}")))
    }
  }

  def spark[A](block: SparkContext => A): A = {
    val sc = SparkContext.getOrCreate(new SparkConf().setAppName("").setMaster("local[*]"))
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

  def executeJob(classpath: File): Unit = {
    val cl = Thread.currentThread.getContextClassLoader
    try {
      val classloader = new URLClassLoader(Array(classpath.toURI.toURL), cl)
      Thread.currentThread.setContextClassLoader(classloader)
      val cls = Class.forName(
        s"${GeneratedClassPackageInternalName.replaceAll("/", ".")}.flowId.SparkClient",
        true, classloader)
        .asSubclass(classOf[SparkClient])
      val instance = cls.newInstance()

      val conf = new SparkConf()
      conf.setAppName("AsakusaSparkClient")
      conf.setMaster("local[8]")

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
}
