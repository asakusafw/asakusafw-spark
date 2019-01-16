/*
 * Copyright 2011-2019 Asakusa Framework Team.
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
package com.asakusafw.spark.extensions.iterativebatch.runtime
package graph

import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner

import java.io.{ DataInput, DataOutput, File }

import scala.concurrent.{ Await, ExecutionContext, Future }
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{ NullWritable, Writable }
import org.apache.spark.SparkConf

import com.asakusafw.bridge.stage.StageInfo
import com.asakusafw.runtime.directio.{ Counter, DataDefinition, OutputAttemptContext }
import com.asakusafw.runtime.directio.hadoop.{ HadoopDataSource, HadoopDataSourceUtil, SequenceFileFormat }
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.runtime.value.IntOption
import com.asakusafw.spark.runtime._
import com.asakusafw.spark.runtime.directio.{ BasicDataDefinition, HadoopObjectFactory }
import com.asakusafw.spark.runtime.graph.CacheOnce

import resource._

@RunWith(classOf[JUnitRunner])
class DirectOutputCommitForIterativeSpecTest extends DirectOutputCommitForIterativeSpec

class DirectOutputCommitForIterativeSpec
  extends FlatSpec
  with SparkForAll
  with JobContextSugar
  with RoundContextSugar
  with TempDirForAll {

  import DirectOutputCommitForIterativeSpec._

  behavior of classOf[DirectOutputCommitForIterative].getSimpleName

  private var root: File = _

  override def configure(conf: SparkConf): SparkConf = {
    val tmpDir = createTempDirectoryForAll("directio-").toFile()
    val system = new File(tmpDir, "system")
    root = new File(tmpDir, "directio")
    conf.setHadoopConf("com.asakusafw.output.system.dir", system.getAbsolutePath)
    conf.setHadoopConf("com.asakusafw.directio.test", classOf[HadoopDataSource].getName)
    conf.setHadoopConf("com.asakusafw.directio.test.path", "test")
    conf.setHadoopConf("com.asakusafw.directio.test.fs.path", root.getAbsolutePath)
  }

  it should "commit" in {
    implicit val jobContext = newJobContext(sc)

    val rounds = 0 to 1
    val files = rounds.map { round =>
      new File(root, s"out1_${round}/testing.bin")
    }

    val prepare = new Prepare("id", "test/out1_${round}", "testing.bin")("prepare")
    val commit = new Commit(Set(prepare))(Set("test/out1_${round}"))

    val origin = newRoundContext()
    val rcs = rounds.map { round =>
      newRoundContext(
        stageId = s"round_${round}",
        batchArguments = Map("round" -> round.toString))
    }

    assert(files.exists(_.exists()) === false)

    Await.result(prepare.perform(origin, rcs), Duration.Inf)
    assert(files.exists(_.exists()) === false)

    Await.result(commit.perform(origin, rcs), Duration.Inf)
    assert(files.forall(_.exists()) === true)
  }

  it should "commit out of scope round" in {
    implicit val jobContext = newJobContext(sc)

    val rounds = 0 to 1
    val files = rounds.map { round =>
      new File(root, s"out2_${round}/testing.bin")
    }

    val prepare = new Prepare("id", "test/out2_${round}", "testing.bin")("prepare")
    val commit = new Commit(Set(prepare))(Set("test/out2_${round}"))

    val origin = newRoundContext()
    val rcs = rounds.map { round =>
      newRoundContext(
        stageId = s"round_${round}",
        batchArguments = Map("round" -> round.toString))
    }

    assert(files.exists(_.exists()) === false)

    Await.result(commit.perform(origin, Seq(rcs.head)), Duration.Inf)
    assert(files.head.exists() === true)
    assert(files.tail.exists(_.exists()) === false)

    Await.result(commit.perform(origin, rcs.tail), Duration.Inf)
    assert(files.forall(_.exists()) === true)
  }
}

object DirectOutputCommitForIterativeSpec {

  class Prepare(
    id: String,
    basePath: String,
    resourceName: String)(
      val label: String)(
        implicit val jobContext: JobContext)
    extends IterativeAction[Unit] with CacheAlways[Seq[RoundContext], Future[Unit]] {

    override protected def doPerform(
      origin: RoundContext,
      rcs: Seq[RoundContext])(implicit ec: ExecutionContext): Future[Unit] = Future {
      val conf = origin.hadoopConf.value
      val repository = HadoopDataSourceUtil.loadRepository(conf)

      rcs.foreach { rc =>
        val conf = rc.hadoopConf.value
        val stageInfo = StageInfo.deserialize(conf.get(StageInfo.KEY_NAME))

        val basePath = stageInfo.resolveUserVariables(this.basePath)
        val sourceId = repository.getRelatedId(basePath)
        val containerPath = repository.getContainerPath(basePath)
        val componentPath = repository.getComponentPath(basePath)
        val dataSource = repository.getRelatedDataSource(containerPath)

        val context = new OutputAttemptContext(
          stageInfo.getExecutionId, stageInfo.getStageId, sourceId, new Counter())
        dataSource.setupAttemptOutput(context)

        val definition =
          BasicDataDefinition(new HadoopObjectFactory(conf), classOf[FooSequenceFileFormat])
        for {
          out <- managed(
            dataSource.openOutput(context, definition, componentPath, resourceName, new Counter()))
        } {
          val foo = new Foo()
          foo.id.modify(1)
          out.write(foo)
        }

        dataSource.commitAttemptOutput(context)
        dataSource.cleanupAttemptOutput(context)
      }
    }
  }

  class Commit(
    prepares: Set[IterativeAction[Unit]])(
      val basePaths: Set[String])(
        implicit jobContext: JobContext)
    extends DirectOutputCommitForIterative(prepares)
    with CacheAlways[Seq[RoundContext], Future[Unit]]

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
