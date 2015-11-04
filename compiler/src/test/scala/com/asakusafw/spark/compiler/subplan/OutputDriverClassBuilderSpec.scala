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
package subplan

import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner

import java.io.{ DataInput, DataOutput, File }

import scala.collection.mutable
import scala.collection.JavaConversions._
import scala.concurrent.{ Await, Future }
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{ NullWritable, Writable }
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import com.asakusafw.bridge.stage.StageInfo
import com.asakusafw.lang.compiler.api.CompilerOptions
import com.asakusafw.lang.compiler.api.testing.MockJobflowProcessorContext
import com.asakusafw.lang.compiler.model.description.ClassDescription
import com.asakusafw.lang.compiler.model.graph.{ ExternalOutput, MarkerOperator }
import com.asakusafw.lang.compiler.planning.{ PlanBuilder, PlanMarker }
import com.asakusafw.runtime.compatibility.JobCompatibility
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.runtime.stage.input.TemporaryInputFormat
import com.asakusafw.runtime.value.IntOption
import com.asakusafw.spark.compiler.planning.SubPlanInfo
import com.asakusafw.spark.compiler.spi.SubPlanCompiler
import com.asakusafw.spark.runtime.{ HadoopConfForEach, TempDirForEach }
import com.asakusafw.spark.runtime.driver.OutputDriver
import com.asakusafw.spark.tools.asm._

@RunWith(classOf[JUnitRunner])
class OutputDriverClassBuilderSpecTest extends OutputDriverClassBuilderSpec

class OutputDriverClassBuilderSpec
  extends FlatSpec
  with SparkWithClassServerForAll
  with HadoopConfForEach
  with TempDirForEach
  with UsingCompilerContext {

  import OutputDriverClassBuilderSpec._

  behavior of classOf[OutputDriverClassBuilder].getSimpleName

  def readResult(path: String, hadoopConf: Broadcast[Configuration]): Seq[Int] = {
    val job = JobCompatibility.newJob(hadoopConf.value)

    val stageInfo = StageInfo.deserialize(job.getConfiguration.get(StageInfo.KEY_NAME))
    FileInputFormat.setInputPaths(job, new Path(stageInfo.resolveUserVariables(path + "/part-*")))

    sc.newAPIHadoopRDD(
      job.getConfiguration,
      classOf[TemporaryInputFormat[Foo]],
      classOf[NullWritable],
      classOf[Foo]).map(_._2.id.get).collect.toSeq.sorted
  }

  it should "build output driver class" in {
    val tmpDir = new File(createTempDirectoryForEach("test-").toFile, "tmp").getAbsolutePath

    val jpContext = new MockJobflowProcessorContext(
      new CompilerOptions("buildid", tmpDir, Map.empty[String, String]),
      Thread.currentThread.getContextClassLoader,
      classServer.root.toFile)

    val outputMarker = MarkerOperator.builder(ClassDescription.of(classOf[Foo]))
      .attribute(classOf[PlanMarker], PlanMarker.GATHER).build()
    val endMarker = MarkerOperator.builder(ClassDescription.of(classOf[Foo]))
      .attribute(classOf[PlanMarker], PlanMarker.END).build()

    val outputOperator = ExternalOutput.builder("foos")
      .input(ExternalOutput.PORT_NAME, ClassDescription.of(classOf[Foo]), outputMarker.getOutput)
      .output("end", ClassDescription.of(classOf[Foo]))
      .build()
    outputOperator.findOutput("end").connect(endMarker.getInput)

    val plan = PlanBuilder.from(Seq(outputOperator))
      .add(
        Seq(outputMarker),
        Seq(endMarker)).build().getPlan()
    assert(plan.getElements.size === 1)

    val subplan = plan.getElements.head
    subplan.putAttr(
      new SubPlanInfo(_,
        SubPlanInfo.DriverType.OUTPUT,
        Seq.empty[SubPlanInfo.DriverOption],
        outputOperator))

    implicit val context = newSubPlanCompilerContext(flowId, jpContext)

    val compiler = SubPlanCompiler(subplan.getAttribute(classOf[SubPlanInfo]).getDriverType)
    val thisType = compiler.compile(subplan)
    context.addClass(context.branchKeys)
    context.addClass(context.broadcastIds)
    val cls = classServer.loadClass(thisType).asSubclass(classOf[OutputDriver[Foo]])

    val foos = sc.parallelize(0 until 10).map(Foo.intToFoo)
    val terminators = mutable.Set.empty[Future[Unit]]
    val driver = cls.getConstructor(
      classOf[SparkContext],
      classOf[Broadcast[Configuration]],
      classOf[Seq[Future[RDD[_]]]],
      classOf[mutable.Set[Future[Unit]]])
      .newInstance(
        sc,
        hadoopConf,
        Seq(Future.successful(foos)),
        terminators)

    driver.execute()
    Await.result(Future.sequence(terminators), Duration.Inf)

    val result = readResult(new File(tmpDir, "foos").getAbsolutePath, hadoopConf)
    assert(result === (0 until 10))
  }
}

object OutputDriverClassBuilderSpec {

  class Foo extends DataModel[Foo] with Writable {

    val id = new IntOption()

    override def reset(): Unit = {
      id.setNull()
    }
    override def copyFrom(other: Foo): Unit = {
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

  object Foo {

    def intToFoo: Int => (_, Foo) = {

      lazy val foo = new Foo()

      { i =>
        foo.id.modify(i)
        (foo, foo)
      }
    }
  }
}
