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
package com.asakusafw.spark.runtime
package graph

import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner

import java.io.{ DataInput, DataOutput, File }

import scala.concurrent.{ Await, ExecutionContext, Future }
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

import org.apache.hadoop.io.{ NullWritable, Writable }
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat
import org.apache.spark.{ HashPartitioner, Partitioner, SparkConf }

import com.asakusafw.runtime.directio.DataFormat
import com.asakusafw.runtime.directio.hadoop.{ HadoopDataSource, SequenceFileFormat }
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.runtime.value.{ IntOption, StringOption, ValueOption }
import com.asakusafw.spark.runtime.JobContext.OutputCounter.Direct
import com.asakusafw.spark.runtime.directio.OutputPatternGenerator
import com.asakusafw.spark.runtime.directio.OutputPatternGenerator._
import com.asakusafw.spark.runtime.io.WritableSerDe
import com.asakusafw.spark.runtime.rdd.{ BranchKey, ShuffleKey }

import resource._

@RunWith(classOf[JUnitRunner])
class DirectOutputPrepareSpecTest extends DirectOutputPrepareSpec

class DirectOutputPrepareSpec
  extends FlatSpec
  with SparkForAll
  with JobContextSugar
  with RoundContextSugar
  with TempDirForAll {

  behavior of classOf[DirectOutputPrepare[_]].getSimpleName

  import DirectOutputPrepareSpec._

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

  it should "prepare flat" in {
    implicit val jobContext = newJobContext(sc)

    val numSlices = 2
    val files = (0 until numSlices).map(i => new File(root, s"flat/s0-p${i}.bin"))

    val source =
      new ParallelCollectionSource(Input, 0 until 100, Some(numSlices))("input")
        .map(Input)(Foo.intToFoo)

    val setup = new Setup("setup")
    val prepare = new Flat.Prepare(
      setup,
      Seq((source, Input)))(
      "flat",
      "test/flat",
      "*.bin",
      classOf[FooSequenceFileFormat])(
      "flat")
    val commit = new Commit(prepare)("test/flat")

    val rc = newRoundContext()

    assert(files.exists(_.exists()) === false)

    Await.result(prepare.perform(rc), Duration.Inf)
    assert(files.exists(_.exists()) === false)

    Await.result(commit.perform(rc), Duration.Inf)
    assert(files.forall(_.exists()) === true)

    assert(
      sc.newAPIHadoopFile[NullWritable, Foo, SequenceFileInputFormat[NullWritable, Foo]](
        new File(root, "flat/s0-*.bin").getAbsolutePath)
        .map(_._2)
        .map(foo => (foo.id.get, foo.group.get))
        .collect.toSeq
        === (0 until 100).map(i => (i, i % 10)))

    assert(jobContext.outputStatistics(Direct).size === 1)
    val statistics = jobContext.outputStatistics(Direct)("flat")
    assert(statistics.files === 2)
    assert(statistics.bytes === 2044)
    assert(statistics.records === 100)
  }

  it should "prepare group" in {
    implicit val jobContext = newJobContext(sc)

    val numSlices = 2
    val files = (0 until 10).map(i => new File(root, s"group/foo_${i}.bin"))

    val source =
      new ParallelCollectionSource(Input,
        (0 until 100).map(i => (i, math.random)).sortBy(_._2).map(_._1), Some(numSlices))("input")
        .map(Input)(Foo.intToFoo)

    val setup = new Setup("setup")
    val prepare = new Group.Prepare(
      setup,
      Seq((source, Input)))(
      new HashPartitioner(sc.defaultParallelism))(
      "group",
      "test/group",
      classOf[FooSequenceFileFormat])(
      "group")
    val commit = new Commit(prepare)("test/group")

    val rc = newRoundContext()

    assert(files.exists(_.exists()) === false)

    Await.result(prepare.perform(rc), Duration.Inf)
    assert(files.exists(_.exists()) === false)

    Await.result(commit.perform(rc), Duration.Inf)
    assert(files.forall(_.exists()) === true)

    files.zipWithIndex.foreach {
      case (file, i) =>
        assert(
          sc.newAPIHadoopFile[NullWritable, Foo, SequenceFileInputFormat[NullWritable, Foo]](
            file.getAbsolutePath)
            .map(_._2)
            .map(foo => (foo.id.get, foo.group.get))
            .collect.toSeq
            === (0 until 100).reverse.filter(_ % 10 == i).map(j => (j, i)))
    }

    assert(jobContext.outputStatistics(Direct).size === 1)
    val statistics = jobContext.outputStatistics(Direct)("group")
    assert(statistics.files === 10)
    assert(statistics.bytes === 3020)
    assert(statistics.records === 100)
  }
}

object DirectOutputPrepareSpec {

  class Setup(
    val label: String)(
      implicit val jobContext: JobContext)
    extends Action[Unit] with CacheOnce[RoundContext, Future[Unit]] {

    override protected def doPerform(
      rc: RoundContext)(implicit ec: ExecutionContext): Future[Unit] = Future {}
  }

  class Commit(
    prepares: Set[Action[Unit]])(
      val basePaths: Set[String])(
        implicit jobContext: JobContext)
    extends DirectOutputCommit(prepares) with CacheOnce[RoundContext, Future[Unit]] {

    def this(
      prepare: Action[Unit])(
        basePath: String)(
          implicit jobContext: JobContext) = this(Set(prepare))(Set(basePath))
  }

  val Input = BranchKey(0)

  class Foo extends DataModel[Foo] with Writable {

    val id = new IntOption()
    val group = new IntOption()

    override def reset(): Unit = {
      id.setNull()
      group.setNull()
    }
    override def copyFrom(other: Foo): Unit = {
      id.copyFrom(other.id)
      group.copyFrom(other.group)
    }
    override def readFields(in: DataInput): Unit = {
      id.readFields(in)
      group.readFields(in)
    }
    override def write(out: DataOutput): Unit = {
      id.write(out)
      group.write(out)
    }

    override def toString: String = s"Foo(id=${id}, group=${group})"
  }

  object Foo {

    def intToFoo: Int => (_, Foo) = {

      lazy val foo = new Foo()

      { i =>
        foo.id.modify(i)
        foo.group.modify(i % 10)
        (NullWritable.get, foo)
      }
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

  object Flat {

    class Prepare(
      setup: Action[Unit],
      prevs: Seq[(Source, BranchKey)])(
        val name: String,
        val basePath: String,
        val resourcePattern: String,
        val formatType: Class[_ <: DataFormat[Foo]])(
          val label: String)(
            implicit jobContext: JobContext)
      extends DirectOutputPrepareFlat[Foo](setup, prevs)
      with CacheOnce[RoundContext, Future[Unit]]
  }

  object Group {

    class Prepare(
      setup: Action[Unit],
      prevs: Seq[(Source, BranchKey)])(
        partitioner: Partitioner)(
          val name: String,
          val basePath: String,
          val formatType: Class[_ <: DataFormat[Foo]])(
            val label: String)(
              implicit jobContext: JobContext)
      extends DirectOutputPrepareGroup[Foo](setup, prevs)(partitioner)
      with CacheOnce[RoundContext, Future[Unit]] {

      override lazy val outputPatternGenerator = new FooOutputPatternGenerator(
        Seq(constant("foo_"), natural("group"), constant(".bin")))

      override val sortOrdering: SortOrdering = new Ord

      override def orderings(value: Foo): Seq[ValueOption[_]] = Seq(value.id)

      override def newDataModel(): Foo = new Foo()
    }

    class FooOutputPatternGenerator(fragments: Seq[Fragment])
      extends OutputPatternGenerator[Foo](fragments) {

      override def getProperty(foo: Foo, property: String): ValueOption[_] = {
        property match {
          case "id" => foo.id
          case "group" => foo.group
        }
      }
    }

    class Ord extends Ordering[ShuffleKey] {

      override def compare(x: ShuffleKey, y: ShuffleKey): Int = {
        val xGrouping = x.grouping
        val yGrouping = y.grouping
        val cmp = StringOption.compareBytes(xGrouping, 0, xGrouping.length, yGrouping, 0, yGrouping.length)
        if (cmp == 0) {
          val xOrdering = x.ordering
          val yOrdering = y.ordering
          IntOption.compareBytes(yOrdering, 0, yOrdering.length, xOrdering, 0, xOrdering.length)
        } else {
          cmp
        }
      }
    }
  }
}
