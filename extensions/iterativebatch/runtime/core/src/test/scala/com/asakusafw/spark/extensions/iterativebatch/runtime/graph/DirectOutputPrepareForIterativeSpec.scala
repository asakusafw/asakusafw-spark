/*
 * Copyright 2011-2018 Asakusa Framework Team.
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

import scala.collection.JavaConversions._
import scala.concurrent.{ Await, ExecutionContext, Future }
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.reflect.ClassTag

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{ NullWritable, Writable }
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat
import org.apache.spark.{ HashPartitioner, Partitioner, SparkConf }
import org.apache.spark.rdd.RDD

import com.asakusafw.bridge.stage.StageInfo
import com.asakusafw.runtime.directio.DataFormat
import com.asakusafw.runtime.directio.hadoop.{ HadoopDataSource, SequenceFileFormat }
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.runtime.value.{ IntOption, StringOption, ValueOption }
import com.asakusafw.spark.runtime._
import com.asakusafw.spark.runtime.JobContext.OutputCounter.Direct
import com.asakusafw.spark.runtime.directio.OutputPatternGenerator
import com.asakusafw.spark.runtime.directio.OutputPatternGenerator._
import com.asakusafw.spark.runtime.graph._
import com.asakusafw.spark.runtime.rdd.{ BranchKey, IdentityPartitioner, ShuffleKey }

@RunWith(classOf[JUnitRunner])
class DirectOutputPrepareForIterativeSpecTest extends DirectOutputPrepareForIterativeSpec

class DirectOutputPrepareForIterativeSpec
  extends FlatSpec
  with SparkForAll
  with JobContextSugar
  with RoundContextSugar
  with TempDirForAll {

  import DirectOutputPrepareForIterativeSpec._

  behavior of classOf[DirectOutputPrepareForIterative[_]].getSimpleName

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

  {
    var stageOffset = 0

    it should "prepare flat simple" in {
      implicit val jobContext = newJobContext(sc)

      val rounds = 0 to 1
      val numSlices = 2
      val files = rounds.map { round =>
        (0 until numSlices).map(i => new File(root, s"flat1_${round}/s${round + stageOffset}-p${i}.bin"))
      }

      val source =
        new RoundAwareParallelCollectionSource(Input, 0 until 100, Some(numSlices))("input")
          .mapWithRoundContext(Input)(Foo.intToFoo)

      val setup = new Setup("setup")
      val prepareEach =
        new Flat.PrepareEach(
          Seq((source, Input)))(
          "test/flat1_${round}",
          "*.bin")(
          "prepare flat.each 1")
      val prepare = new Prepare(
        setup,
        prepareEach)(
        "flat1",
        classOf[FooSequenceFileFormat])(
        "prepare flat 1")
      val commit = new Commit(prepare)("test/flat1_${round}")

      val origin = newRoundContext()
      val rcs = rounds.map { round =>
        newRoundContext(
          stageId = s"round_${round}",
          batchArguments = Map("round" -> round.toString))
      }

      assert(files.exists(_.exists(_.exists())) === false)

      rcs.foreach { rc =>
        Await.result(prepareEach.perform(rc), Duration.Inf)
      }

      Await.result(prepare.perform(origin, rcs), Duration.Inf)
      assert(files.exists(_.exists(_.exists())) === false)

      Await.result(commit.perform(origin, rcs), Duration.Inf)
      assert(files.forall(_.forall(_.exists())) === true)

      rounds.foreach { round =>
        assert(
          sc.newAPIHadoopFile[NullWritable, Foo, SequenceFileInputFormat[NullWritable, Foo]](
            new File(root, s"flat1_${round}/s${round + stageOffset}-*.bin").getAbsolutePath)
            .map(_._2)
            .map(foo => (foo.id.get, foo.group.get))
            .collect.toSeq
            === (0 until 100).map(i => (100 * round + i, i % 10)))
      }

      assert(jobContext.outputStatistics(Direct).size === 1)
      val statistics = jobContext.outputStatistics(Direct)("flat1")
      assert(statistics.files === 4)
      assert(statistics.bytes === 4240)
      assert(statistics.records === 200)

      stageOffset += 7
    }

    it should "prepare flat simple with batch argument" in {
      implicit val jobContext = newJobContext(sc)

      val rounds = 0 to 1
      val numSlices = 2
      val files = rounds.map { round =>
        (0 until numSlices).map(i => new File(root, s"flat1_bar_${round}/bar_s${round + stageOffset}-p${i}.bin"))
      }

      val source =
        new RoundAwareParallelCollectionSource(Input, 0 until 100, Some(numSlices))("input")
          .mapWithRoundContext(Input)(Foo.intToFoo)

      val setup = new Setup("setup")
      val prepareEach =
        new Flat.PrepareEach(
          Seq((source, Input)))(
          "test/flat1_${arg}_${round}",
          "${arg}_*.bin")(
          "prepare flat.each 1 with batch argument")
      val prepare = new Prepare(
        setup,
        prepareEach)(
        "flat1",
        classOf[FooSequenceFileFormat])(
        "prepare flat 1 with batch argument")
      val commit = new Commit(prepare)("test/flat1_${round}")

      val origin = newRoundContext()
      val rcs = rounds.map { round =>
        newRoundContext(
          stageId = s"round_${round}",
          batchArguments = Map("arg" -> "bar", "round" -> round.toString))
      }

      assert(files.exists(_.exists(_.exists())) === false)

      rcs.foreach { rc =>
        Await.result(prepareEach.perform(rc), Duration.Inf)
      }

      Await.result(prepare.perform(origin, rcs), Duration.Inf)
      assert(files.exists(_.exists(_.exists())) === false)

      Await.result(commit.perform(origin, rcs), Duration.Inf)
      assert(files.forall(_.forall(_.exists())) === true)

      rounds.foreach { round =>
        assert(
          sc.newAPIHadoopFile[NullWritable, Foo, SequenceFileInputFormat[NullWritable, Foo]](
            new File(root, s"flat1_bar_${round}/bar_s${round + stageOffset}-*.bin").getAbsolutePath)
            .map(_._2)
            .map(foo => (foo.id.get, foo.group.get))
            .collect.toSeq
            === (0 until 100).map(i => (100 * round + i, i % 10)))
      }

      assert(jobContext.outputStatistics(Direct).size === 1)
      val statistics = jobContext.outputStatistics(Direct)("flat1")
      assert(statistics.files === 4)
      assert(statistics.bytes === 4240)
      assert(statistics.records === 200)

      stageOffset += 7
    }

    it should "prepare flat w/o round variable" in {
      implicit val jobContext = newJobContext(sc)

      val rounds = 0 to 1
      val numSlices = 2
      val files = rounds.map { round =>
        (0 until numSlices).map(i => new File(root, s"flat2/s${round + stageOffset}-p${i}.bin"))
      }

      val source =
        new RoundAwareParallelCollectionSource(Input, 0 until 100, Some(numSlices))("input")
          .mapWithRoundContext(Input)(Foo.intToFoo)

      val setup = new Setup("setup")
      val prepareEach =
        new Flat.PrepareEach(
          Seq((source, Input)))(
          "test/flat2",
          "*.bin")(
          "prepare flat.each 2")
      val prepare = new Prepare(
        setup,
        prepareEach)(
        "flat2",
        classOf[FooSequenceFileFormat])(
        "prepare flat 2")
      val commit = new Commit(prepare)("test/flat2")

      val origin = newRoundContext()
      val rcs = rounds.map { round =>
        newRoundContext(
          stageId = s"round_${round}",
          batchArguments = Map("round" -> round.toString))
      }

      assert(files.exists(_.exists(_.exists())) === false)

      rcs.foreach { rc =>
        Await.result(prepareEach.perform(rc), Duration.Inf)
      }

      Await.result(prepare.perform(origin, rcs), Duration.Inf)
      assert(files.exists(_.exists(_.exists())) === false)

      Await.result(commit.perform(origin, rcs), Duration.Inf)
      assert(files.forall(_.forall(_.exists())) === true)

      rounds.foreach { round =>
        assert(
          sc.newAPIHadoopFile[NullWritable, Foo, SequenceFileInputFormat[NullWritable, Foo]](
            new File(root, s"flat2/s${round + stageOffset}-*.bin").getAbsolutePath)
            .map(_._2)
            .map(foo => (foo.id.get, foo.group.get))
            .collect.toSeq
            === (0 until 100).map(i => (100 * round + i, i % 10)))
      }

      assert(jobContext.outputStatistics(Direct).size === 1)
      val statistics = jobContext.outputStatistics(Direct)("flat2")
      assert(statistics.files === 4)
      assert(statistics.bytes === 4240)
      assert(statistics.records === 200)

      stageOffset += 7
    }

    it should "prepare flat a part of rounds" in {
      implicit val jobContext = newJobContext(sc)

      val rounds = 0 to 1
      val numSlices = 2
      val files = rounds.map { round =>
        (0 until numSlices).map(i => new File(root, s"flat3_${round}/s${round + stageOffset}-p${i}.bin"))
      }

      val source =
        new RoundAwareParallelCollectionSource(Input, 0 until 100, Some(numSlices))("input")
          .mapWithRoundContext(Input)(Foo.intToFoo)

      val setup = new Setup("setup")
      val prepareEach =
        new Flat.PrepareEach(
          Seq((source, Input)))(
          "test/flat3_${round}",
          "*.bin")(
          "prepare flat.each 3")
      val prepare = new Prepare(
        setup,
        prepareEach)(
        "flat3",
        classOf[FooSequenceFileFormat])(
        "prepare flat 3")
      val commit = new Commit(prepare)("test/flat3_${round}")

      val origin = newRoundContext()
      val rcs = rounds.map { round =>
        newRoundContext(
          stageId = s"round_${round}",
          batchArguments = Map("round" -> round.toString))
      }

      assert(files.exists(_.exists(_.exists())) === false)

      rcs.foreach { rc =>
        Await.result(prepareEach.perform(rc), Duration.Inf)
      }

      val head = Seq(rcs.head)

      Await.result(prepare.perform(origin, head), Duration.Inf)
      assert(files.exists(_.exists(_.exists())) === false)

      Await.result(commit.perform(origin, head), Duration.Inf)
      assert(files.head.forall(_.exists()) === true)
      assert(files.tail.exists(_.exists(_.exists())) === false)

      assert(jobContext.outputStatistics(Direct).size === 1)
      val statistics = jobContext.outputStatistics(Direct)("flat3")
      assert(statistics.files === 2)
      assert(statistics.bytes === 2120)
      assert(statistics.records === 100)

      Await.result(commit.perform(origin, rcs.tail), Duration.Inf)
      assert(files.forall(_.forall(_.exists())) === true)

      rounds.foreach { round =>
        assert(
          sc.newAPIHadoopFile[NullWritable, Foo, SequenceFileInputFormat[NullWritable, Foo]](
            new File(root, s"flat3_${round}/s${round + stageOffset}-*.bin").getAbsolutePath)
            .map(_._2)
            .map(foo => (foo.id.get, foo.group.get))
            .collect.toSeq
            === (0 until 100).map(i => (100 * round + i, i % 10)))
      }

      assert(statistics.files === 4)
      assert(statistics.bytes === 4240)
      assert(statistics.records === 200)
    }
  }

  it should "prepare group simple" in {
    implicit val jobContext = newJobContext(sc)

    val rounds = 0 to 1
    val numSlices = 2
    val files = rounds.map { round =>
      (0 until numSlices).map(i => new File(root, s"group1_${round}/foo_${i}.bin"))
    }

    val source =
      new RoundAwareParallelCollectionSource(Input, 0 until 100, Some(numSlices))("input")
        .mapWithRoundContext(Input)(Foo.intToFoo)

    val setup = new Setup("setup")
    val prepareEach =
      new Group.PrepareEach(
        Seq((source, Input)))(
        new HashPartitioner(sc.defaultParallelism))(
        "test/group1_${round}",
        Seq(constant("foo_"), natural("group"), constant(".bin")))(
        "prepare group.each 1")
    val prepare = new Prepare(
      setup,
      prepareEach)(
      "group1",
      classOf[FooSequenceFileFormat])(
      "prepare group 1")
    val commit = new Commit(prepare)("test/group1_${round}")

    val origin = newRoundContext()
    val rcs = rounds.map { round =>
      newRoundContext(
        stageId = s"round_${round}",
        batchArguments = Map("round" -> round.toString))
    }

    assert(files.exists(_.exists(_.exists())) === false)

    rcs.foreach { rc =>
      Await.result(prepareEach.perform(rc), Duration.Inf)
    }

    Await.result(prepare.perform(origin, rcs), Duration.Inf)
    assert(files.exists(_.exists(_.exists())) === false)

    Await.result(commit.perform(origin, rcs), Duration.Inf)
    assert(files.forall(_.forall(_.exists())) === true)

    rounds.zip(files).foreach {
      case (round, files) =>
        files.zipWithIndex.foreach {
          case (file, i) =>
            assert(
              sc.newAPIHadoopFile[NullWritable, Foo, SequenceFileInputFormat[NullWritable, Foo]](
                file.getAbsolutePath)
                .map(_._2)
                .map(foo => (foo.id.get, foo.group.get))
                .collect.toSeq
                === (0 until 100).reverse.filter(_ % 10 == i).map(j => (100 * round + j, i)))
        }
    }

    assert(jobContext.outputStatistics(Direct).size === 1)
    val statistics = jobContext.outputStatistics(Direct)("group1")
    assert(statistics.files === 20)
    assert(statistics.bytes === 6800)
    assert(statistics.records === 200)
  }

  it should "prepare group simple with batch argument" in {
    implicit val jobContext = newJobContext(sc)

    val rounds = 0 to 1
    val numSlices = 2
    val files = rounds.map { round =>
      (0 until numSlices).map(i => new File(root, s"group1_bar_${round}/foo_bar_${i}.bin"))
    }

    val source =
      new RoundAwareParallelCollectionSource(Input, 0 until 100, Some(numSlices))("input")
        .mapWithRoundContext(Input)(Foo.intToFoo)

    val setup = new Setup("setup")
    val prepareEach =
      new Group.PrepareEach(
        Seq((source, Input)))(
        new HashPartitioner(sc.defaultParallelism))(
        "test/group1_${arg}_${round}",
        Seq(constant("foo_${arg}_"), natural("group"), constant(".bin")))(
        "prepare group.each 1 with batch argument")
    val prepare = new Prepare(
      setup,
      prepareEach)(
      "group1",
      classOf[FooSequenceFileFormat])(
      "prepare group 1 with batch argument")
    val commit = new Commit(prepare)("test/group1_${round}")

    val origin = newRoundContext()
    val rcs = rounds.map { round =>
      newRoundContext(
        stageId = s"round_${round}",
        batchArguments = Map("arg" -> "bar", "round" -> round.toString))
    }

    assert(files.exists(_.exists(_.exists())) === false)

    rcs.foreach { rc =>
      Await.result(prepareEach.perform(rc), Duration.Inf)
    }

    Await.result(prepare.perform(origin, rcs), Duration.Inf)
    assert(files.exists(_.exists(_.exists())) === false)

    Await.result(commit.perform(origin, rcs), Duration.Inf)
    assert(files.forall(_.forall(_.exists())) === true)

    rounds.zip(files).foreach {
      case (round, files) =>
        files.zipWithIndex.foreach {
          case (file, i) =>
            assert(
              sc.newAPIHadoopFile[NullWritable, Foo, SequenceFileInputFormat[NullWritable, Foo]](
                file.getAbsolutePath)
                .map(_._2)
                .map(foo => (foo.id.get, foo.group.get))
                .collect.toSeq
                === (0 until 100).reverse.filter(_ % 10 == i).map(j => (100 * round + j, i)))
        }
    }

    assert(jobContext.outputStatistics(Direct).size === 1)
    val statistics = jobContext.outputStatistics(Direct)("group1")
    assert(statistics.files === 20)
    assert(statistics.bytes === 6800)
    assert(statistics.records === 200)
  }

  it should "prepare group w/o round variable" in {
    implicit val jobContext = newJobContext(sc)

    val rounds = 0 to 1
    val numSlices = 2
    val files = (0 until numSlices).map(i => new File(root, s"group2/foo_${i}.bin"))

    val source =
      new RoundAwareParallelCollectionSource(Input, 0 until 100, Some(numSlices))("input")
        .mapWithRoundContext(Input)(Foo.intToFoo)

    val setup = new Setup("setup")
    val prepareEach =
      new Group.PrepareEach(
        Seq((source, Input)))(
        new HashPartitioner(sc.defaultParallelism))(
        "test/group2",
        Seq(constant("foo_"), natural("group"), constant(".bin")))(
        "prepare group.each 2")
    val prepare = new Prepare(
      setup,
      prepareEach)(
      "group2",
      classOf[FooSequenceFileFormat])(
      "prepare group 2")
    val commit = new Commit(prepare)("test/group2")

    val origin = newRoundContext()
    val rcs = rounds.map { round =>
      newRoundContext(
        stageId = s"round_${round}",
        batchArguments = Map("round" -> round.toString))
    }

    assert(files.exists(_.exists()) === false)

    rcs.foreach { rc =>
      Await.result(prepareEach.perform(rc), Duration.Inf)
    }

    Await.result(prepare.perform(origin, rcs), Duration.Inf)
    assert(files.exists(_.exists()) === false)

    Await.result(commit.perform(origin, rcs), Duration.Inf)
    assert(files.forall(_.exists()) === true)

    files.zipWithIndex.foreach {
      case (file, i) =>
        assert(
          sc.newAPIHadoopFile[NullWritable, Foo, SequenceFileInputFormat[NullWritable, Foo]](
            file.getAbsolutePath)
            .map(_._2)
            .map(foo => (foo.id.get, foo.group.get))
            .collect.toSeq
            === rounds.reverse.flatMap { round =>
              (0 until 100).reverse.filter(_ % 10 == i).map(j => (100 * round + j, i))
            })
    }

    assert(jobContext.outputStatistics(Direct).size === 1)
    val statistics = jobContext.outputStatistics(Direct)("group2")
    assert(statistics.files === 10)
    assert(statistics.bytes === 5200)
    assert(statistics.records === 200)
  }

  it should "prepare group a part of rounds" in {
    implicit val jobContext = newJobContext(sc)

    val rounds = 0 to 1
    val numSlices = 2
    val files = rounds.map { round =>
      (0 until numSlices).map(i => new File(root, s"group3_${round}/foo_${i}.bin"))
    }

    val source =
      new RoundAwareParallelCollectionSource(Input, 0 until 100, Some(numSlices))("input")
        .mapWithRoundContext(Input)(Foo.intToFoo)

    val setup = new Setup("setup")
    val prepareEach =
      new Group.PrepareEach(
        Seq((source, Input)))(
        new HashPartitioner(sc.defaultParallelism))(
        "test/group3_${round}",
        Seq(constant("foo_"), natural("group"), constant(".bin")))(
        "prepare group.each 3")
    val prepare = new Prepare(
      setup,
      prepareEach)(
      "group3",
      classOf[FooSequenceFileFormat])(
      "prepare group 3")
    val commit = new Commit(prepare)("test/group3_${round}")

    val origin = newRoundContext()
    val rcs = rounds.map { round =>
      newRoundContext(
        stageId = s"round_${round}",
        batchArguments = Map("round" -> round.toString))
    }

    assert(files.exists(_.exists(_.exists())) === false)

    rcs.foreach { rc =>
      Await.result(prepareEach.perform(rc), Duration.Inf)
    }

    val head = Seq(rcs.head)

    Await.result(prepare.perform(origin, head), Duration.Inf)
    assert(files.exists(_.exists(_.exists())) === false)

    Await.result(commit.perform(origin, head), Duration.Inf)
    assert(files.head.forall(_.exists()) === true)
    assert(files.tail.exists(_.exists(_.exists())) === false)

    assert(jobContext.outputStatistics(Direct).size === 1)
    val statistics = jobContext.outputStatistics(Direct)("group3")
    assert(statistics.files === 10)
    assert(statistics.bytes === 3400)
    assert(statistics.records === 100)

    Await.result(commit.perform(origin, rcs.tail), Duration.Inf)
    assert(files.forall(_.forall(_.exists())) === true)

    rounds.zip(files).foreach {
      case (round, files) =>
        files.zipWithIndex.foreach {
          case (file, i) =>
            assert(
              sc.newAPIHadoopFile[NullWritable, Foo, SequenceFileInputFormat[NullWritable, Foo]](
                file.getAbsolutePath)
                .map(_._2)
                .map(foo => (foo.id.get, foo.group.get))
                .collect.toSeq
                === (0 until 100).reverse.filter(_ % 10 == i).map(j => (100 * round + j, i)))
        }
    }

    assert(statistics.files === 20)
    assert(statistics.bytes === 6800)
    assert(statistics.records === 200)
  }
}

object DirectOutputPrepareForIterativeSpec {

  class Setup(
    val label: String)(
      implicit val jobContext: JobContext)
    extends IterativeAction[Unit] with CacheAlways[Seq[RoundContext], Future[Unit]] {

    override protected def doPerform(
      origin: RoundContext,
      rcs: Seq[RoundContext])(implicit ec: ExecutionContext): Future[Unit] = Future {}
  }

  class Commit(
    prepares: Set[IterativeAction[Unit]])(
      val basePaths: Set[String])(
        implicit jobContext: JobContext)
    extends DirectOutputCommitForIterative(prepares)
    with CacheAlways[Seq[RoundContext], Future[Unit]] {

    def this(
      prepare: IterativeAction[Unit])(
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

    def intToFoo(rc: RoundContext): Int => (_, Foo) = {

      val stageInfo = StageInfo.deserialize(rc.hadoopConf.value.get(StageInfo.KEY_NAME))
      val round = stageInfo.getBatchArguments()("round").toInt

      lazy val foo = new Foo()

      { i =>
        foo.id.modify(100 * round + i)
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

  class Prepare(
    setup: IterativeAction[Unit],
    prepare: DirectOutputPrepareEachForIterative[Foo])(
      val name: String,
      val formatType: Class[_ <: DataFormat[Foo]])(
        val label: String)(
          implicit jobContext: JobContext)
    extends DirectOutputPrepareForIterative[Foo](setup, prepare)
    with CacheAlways[Seq[RoundContext], Future[Unit]]

  object Flat {

    class PrepareEach(
      prevs: Seq[(Source, BranchKey)])(
        val basePath: String,
        val resourcePattern: String)(
          val label: String)(
            implicit jobContext: JobContext)
      extends DirectOutputPrepareFlatEachForIterative[Foo](prevs)
      with CacheAlways[RoundContext, Future[RDD[(ShuffleKey, Foo)]]] {

      override def newDataModel(): Foo = new Foo()
    }
  }

  object Group {

    class PrepareEach(
      prevs: Seq[(Source, BranchKey)])(
        partitioner: Partitioner)(
          val basePath: String,
          val fragments: Seq[Fragment])(
            val label: String)(
              implicit jobContext: JobContext)
      extends DirectOutputPrepareGroupEachForIterative[Foo](prevs)(partitioner)
      with CacheAlways[RoundContext, Future[RDD[(ShuffleKey, Foo)]]] {

      override def newDataModel(): Foo = new Foo()

      override lazy val outputPatternGenerator = new FooOutputPatternGenerator(fragments)

      override val sortOrdering: SortOrdering = new Ord

      override def orderings(value: Foo): Seq[ValueOption[_]] = Seq(value.id)
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
        val cmp = StringOption.compareBytes(
          x.grouping, 0, x.grouping.length,
          y.grouping, 0, y.grouping.length)
        if (cmp == 0) {
          val cmp = StringOption.compareBytes(
            x.grouping, StringOption.getBytesLength(x.grouping, 0, x.grouping.length), x.grouping.length,
            y.grouping, StringOption.getBytesLength(y.grouping, 0, y.grouping.length), y.grouping.length)
          if (cmp == 0) {
            val xOrdering = x.ordering
            val yOrdering = y.ordering
            IntOption.compareBytes(
              yOrdering, 0, yOrdering.length,
              xOrdering, 0, xOrdering.length)
          } else {
            cmp
          }
        } else {
          cmp
        }
      }
    }
  }
}
