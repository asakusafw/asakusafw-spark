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
package graph

import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner

import java.io.{ DataInput, DataOutput, File }

import scala.collection.JavaConversions._
import scala.concurrent.{ Await, ExecutionContext, Future }
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

import org.apache.hadoop.io.{ NullWritable, Writable }
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat
import org.apache.spark.{ HashPartitioner, Partitioner, SparkConf }

import com.asakusafw.lang.compiler.extension.directio.{
  DirectFileIoConstants,
  DirectFileOutputModel
}
import com.asakusafw.lang.compiler.model.description.{ ClassDescription, Descriptions }
import com.asakusafw.lang.compiler.model.graph.{ ExternalOutput, MarkerOperator }
import com.asakusafw.lang.compiler.model.info.ExternalOutputInfo
import com.asakusafw.lang.compiler.planning.{ PlanBuilder, PlanMarker }
import com.asakusafw.runtime.directio.hadoop.{ HadoopDataSource, SequenceFileFormat }
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.runtime.value.{ IntOption, StringOption, ValueOption }
import com.asakusafw.spark.compiler.directio.DirectOutputDescription
import com.asakusafw.spark.compiler.graph.DirectOutputPrepareClassBuilderSpec._
import com.asakusafw.spark.compiler.planning.SubPlanInfo
import com.asakusafw.spark.compiler.spi.NodeCompiler
import com.asakusafw.spark.runtime._
import com.asakusafw.spark.runtime.JobContext.OutputCounter.Direct
import com.asakusafw.spark.runtime.directio.OutputPatternGenerator
import com.asakusafw.spark.runtime.directio.OutputPatternGenerator.Fragment
import com.asakusafw.spark.runtime.{ graph => runtime }
import com.asakusafw.spark.runtime.graph._
import com.asakusafw.spark.runtime.rdd.{ BranchKey, ShuffleKey }

@RunWith(classOf[JUnitRunner])
class DirectOutputPrepareClassBuilderSpecTest extends DirectOutputPrepareClassBuilderSpec

class DirectOutputPrepareClassBuilderSpec
  extends FlatSpec
  with ClassServerForAll
  with SparkForAll
  with FlowIdForEach
  with UsingCompilerContext
  with JobContextSugar
  with RoundContextSugar
  with TempDirForAll {

  behavior of "DirectOutputPrepareClassBuilder"

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

    it should "compile prepare flat" in {
      val foosMarker = MarkerOperator.builder(ClassDescription.of(classOf[Foo]))
        .attribute(classOf[PlanMarker], PlanMarker.GATHER).build()
      val endMarker = MarkerOperator.builder(ClassDescription.of(classOf[Foo]))
        .attribute(classOf[PlanMarker], PlanMarker.END).build()

      val outputOperator = ExternalOutput.builder(
        "flat",
        new ExternalOutputInfo.Basic(
          ClassDescription.of(classOf[DirectFileOutputModel]),
          DirectFileIoConstants.MODULE_NAME,
          ClassDescription.of(classOf[Foo]),
          Descriptions.valueOf(
            new DirectFileOutputModel(
              DirectOutputDescription(
                basePath = "test/flat",
                resourcePattern = "*.bin",
                order = Seq.empty,
                deletePatterns = Seq("*.bin"),
                formatType = classOf[FooSequenceFileFormat])))))
        .input(ExternalOutput.PORT_NAME, ClassDescription.of(classOf[Foo]), foosMarker.getOutput)
        .output("end", ClassDescription.of(classOf[Foo]))
        .build()
      outputOperator.findOutput("end").connect(endMarker.getInput)

      val plan = PlanBuilder.from(Seq(outputOperator))
        .add(
          Seq(foosMarker),
          Seq(endMarker)).build().getPlan()
      assert(plan.getElements.size === 1)

      val subplan = plan.getElements.head
      subplan.putAttr(
        new SubPlanInfo(_,
          SubPlanInfo.DriverType.OUTPUT,
          Seq.empty[SubPlanInfo.DriverOption],
          outputOperator))

      val foosInput = subplan.findIn(foosMarker)

      implicit val context = newNodeCompilerContext(flowId, classServer.root.toFile)
      context.branchKeys.getField(foosInput.getOperator.getSerialNumber)

      val compiler = NodeCompiler.get(subplan)
      val thisType = compiler.compile(subplan)
      context.addClass(context.branchKeys)
      context.addClass(context.broadcastIds)
      val cls = classServer.loadClass(thisType).asSubclass(classOf[DirectOutputPrepare[Foo]])

      val numSlices = 2
      val files = (0 until numSlices).map(i => new File(root, s"flat/s${stageOffset}-p${i}.bin"))

      implicit val jobContext = newJobContext(sc)

      val source =
        new ParallelCollectionSource(Input, 0 until 100, Some(numSlices))("input")
          .map(Input)(Foo.intToFoo)

      val setup = new Setup("setup")
      val prepare = cls.getConstructor(
        classOf[Action[Unit]],
        classOf[Seq[(Source, BranchKey)]],
        classOf[JobContext])
        .newInstance(
          setup,
          Seq((source, Input)),
          jobContext)
      val commit = new Commit(prepare)("test/flat")

      val rc = newRoundContext(flowId = flowId)

      assert(files.exists(_.exists()) === false)

      Await.result(prepare.perform(rc), Duration.Inf)
      assert(files.exists(_.exists()) === false)

      Await.result(commit.perform(rc), Duration.Inf)
      assert(files.forall(_.exists()) === true)

      assert(
        sc.newAPIHadoopFile[NullWritable, Foo, SequenceFileInputFormat[NullWritable, Foo]](
          new File(root, s"flat/s${stageOffset}-*.bin").getAbsolutePath)
          .map(_._2)
          .map(foo => (foo.id.get, foo.group.get))
          .collect.toSeq
          === (0 until 100).map(i => (i, i % 10)))

      assert(jobContext.outputStatistics(Direct).size === 1)
      val statistics = jobContext.outputStatistics(Direct)(outputOperator.getName)
      assert(statistics.files === 2)
      assert(statistics.bytes === 2070)
      assert(statistics.records === 100)

      stageOffset += 2
    }

    it should "compile prepare flat with batch argument" in {
      val foosMarker = MarkerOperator.builder(ClassDescription.of(classOf[Foo]))
        .attribute(classOf[PlanMarker], PlanMarker.GATHER).build()
      val endMarker = MarkerOperator.builder(ClassDescription.of(classOf[Foo]))
        .attribute(classOf[PlanMarker], PlanMarker.END).build()

      val outputOperator = ExternalOutput.builder(
        "flat",
        new ExternalOutputInfo.Basic(
          ClassDescription.of(classOf[DirectFileOutputModel]),
          DirectFileIoConstants.MODULE_NAME,
          ClassDescription.of(classOf[Foo]),
          Descriptions.valueOf(
            new DirectFileOutputModel(
              DirectOutputDescription(
                basePath = "test/flat_${arg}",
                resourcePattern = "${arg}_*.bin",
                order = Seq.empty,
                deletePatterns = Seq("*.bin"),
                formatType = classOf[FooSequenceFileFormat])))))
        .input(ExternalOutput.PORT_NAME, ClassDescription.of(classOf[Foo]), foosMarker.getOutput)
        .output("end", ClassDescription.of(classOf[Foo]))
        .build()
      outputOperator.findOutput("end").connect(endMarker.getInput)

      val plan = PlanBuilder.from(Seq(outputOperator))
        .add(
          Seq(foosMarker),
          Seq(endMarker)).build().getPlan()
      assert(plan.getElements.size === 1)

      val subplan = plan.getElements.head
      subplan.putAttr(
        new SubPlanInfo(_,
          SubPlanInfo.DriverType.OUTPUT,
          Seq.empty[SubPlanInfo.DriverOption],
          outputOperator))

      val foosInput = subplan.findIn(foosMarker)

      implicit val context = newNodeCompilerContext(flowId, classServer.root.toFile)
      context.branchKeys.getField(foosInput.getOperator.getSerialNumber)

      val compiler = NodeCompiler.get(subplan)
      val thisType = compiler.compile(subplan)
      context.addClass(context.branchKeys)
      context.addClass(context.broadcastIds)
      val cls = classServer.loadClass(thisType).asSubclass(classOf[DirectOutputPrepare[Foo]])

      val numSlices = 2
      val files = (0 until numSlices).map(i => new File(root, s"flat_bar/bar_s${stageOffset}-p${i}.bin"))

      implicit val jobContext = newJobContext(sc)

      val source =
        new ParallelCollectionSource(Input, 0 until 100, Some(numSlices))("input")
          .map(Input)(Foo.intToFoo)

      val setup = new Setup("setup")
      val prepare = cls.getConstructor(
        classOf[Action[Unit]],
        classOf[Seq[(Source, BranchKey)]],
        classOf[JobContext])
        .newInstance(
          setup,
          Seq((source, Input)),
          jobContext)
      val commit = new Commit(prepare)("test/flat")

      val rc = newRoundContext(flowId = flowId, batchArguments = Map("arg" -> "bar"))

      assert(files.exists(_.exists()) === false)

      Await.result(prepare.perform(rc), Duration.Inf)
      assert(files.exists(_.exists()) === false)

      Await.result(commit.perform(rc), Duration.Inf)
      assert(files.forall(_.exists()) === true)

      assert(
        sc.newAPIHadoopFile[NullWritable, Foo, SequenceFileInputFormat[NullWritable, Foo]](
          new File(root, s"flat_bar/bar_s${stageOffset}-*.bin").getAbsolutePath)
          .map(_._2)
          .map(foo => (foo.id.get, foo.group.get))
          .collect.toSeq
          === (0 until 100).map(i => (i, i % 10)))

      assert(jobContext.outputStatistics(Direct).size === 1)
      val statistics = jobContext.outputStatistics(Direct)(outputOperator.getName)
      assert(statistics.files === 2)
      assert(statistics.bytes === 2070)
      assert(statistics.records === 100)
    }
  }

  it should "compile prepare group" in {
    val foosMarker = MarkerOperator.builder(ClassDescription.of(classOf[Foo]))
      .attribute(classOf[PlanMarker], PlanMarker.GATHER).build()
    val endMarker = MarkerOperator.builder(ClassDescription.of(classOf[Foo]))
      .attribute(classOf[PlanMarker], PlanMarker.END).build()

    val outputOperator = ExternalOutput.builder(
      "group",
      new ExternalOutputInfo.Basic(
        ClassDescription.of(classOf[DirectFileOutputModel]),
        DirectFileIoConstants.MODULE_NAME,
        ClassDescription.of(classOf[Foo]),
        Descriptions.valueOf(
          new DirectFileOutputModel(
            DirectOutputDescription(
              basePath = "test/group",
              resourcePattern = "foo_{group}.bin",
              order = Seq("-id"),
              deletePatterns = Seq("*.bin"),
              formatType = classOf[FooSequenceFileFormat])))))
      .input(ExternalOutput.PORT_NAME, ClassDescription.of(classOf[Foo]), foosMarker.getOutput)
      .output("end", ClassDescription.of(classOf[Foo]))
      .build()
    outputOperator.findOutput("end").connect(endMarker.getInput)

    val plan = PlanBuilder.from(Seq(outputOperator))
      .add(
        Seq(foosMarker),
        Seq(endMarker)).build().getPlan()
    assert(plan.getElements.size === 1)

    val subplan = plan.getElements.head
    subplan.putAttr(
      new SubPlanInfo(_,
        SubPlanInfo.DriverType.OUTPUT,
        Seq.empty[SubPlanInfo.DriverOption],
        outputOperator))

    val foosInput = subplan.findIn(foosMarker)

    implicit val context = newNodeCompilerContext(flowId, classServer.root.toFile)
    context.branchKeys.getField(foosInput.getOperator.getSerialNumber)

    val compiler = NodeCompiler.get(subplan)
    val thisType = compiler.compile(subplan)
    context.addClass(context.branchKeys)
    context.addClass(context.broadcastIds)
    val cls = classServer.loadClass(thisType).asSubclass(classOf[DirectOutputPrepare[Foo]])

    val numSlices = 2
    val files = (0 until 10).map(i => new File(root, s"group/foo_${i}.bin"))

    implicit val jobContext = newJobContext(sc)

    val source =
      new ParallelCollectionSource(Input,
        (0 until 100).map(i => (i, math.random)).sortBy(_._2).map(_._1), Some(numSlices))("input")
        .map(Input)(Foo.intToFoo)

    val setup = new Setup("setup")
    val prepare = cls.getConstructor(
      classOf[Action[Unit]],
      classOf[Seq[(Source, BranchKey)]],
      classOf[Partitioner],
      classOf[JobContext])
      .newInstance(
        setup,
        Seq((source, Input)),
        new HashPartitioner(sc.defaultParallelism),
        jobContext)
    val commit = new Commit(prepare)("test/group")

    val rc = newRoundContext(flowId = flowId)

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
    val statistics = jobContext.outputStatistics(Direct)(outputOperator.getName)
    assert(statistics.files === 10)
    assert(statistics.bytes === 3150)
    assert(statistics.records === 100)
  }

  it should "compile prepare group with batch argument" in {
    val foosMarker = MarkerOperator.builder(ClassDescription.of(classOf[Foo]))
      .attribute(classOf[PlanMarker], PlanMarker.GATHER).build()
    val endMarker = MarkerOperator.builder(ClassDescription.of(classOf[Foo]))
      .attribute(classOf[PlanMarker], PlanMarker.END).build()

    val outputOperator = ExternalOutput.builder(
      "group",
      new ExternalOutputInfo.Basic(
        ClassDescription.of(classOf[DirectFileOutputModel]),
        DirectFileIoConstants.MODULE_NAME,
        ClassDescription.of(classOf[Foo]),
        Descriptions.valueOf(
          new DirectFileOutputModel(
            DirectOutputDescription(
              basePath = "test/group_${arg}",
              resourcePattern = "foo_${arg}_{group}.bin",
              order = Seq("-id"),
              deletePatterns = Seq("*.bin"),
              formatType = classOf[FooSequenceFileFormat])))))
      .input(ExternalOutput.PORT_NAME, ClassDescription.of(classOf[Foo]), foosMarker.getOutput)
      .output("end", ClassDescription.of(classOf[Foo]))
      .build()
    outputOperator.findOutput("end").connect(endMarker.getInput)

    val plan = PlanBuilder.from(Seq(outputOperator))
      .add(
        Seq(foosMarker),
        Seq(endMarker)).build().getPlan()
    assert(plan.getElements.size === 1)

    val subplan = plan.getElements.head
    subplan.putAttr(
      new SubPlanInfo(_,
        SubPlanInfo.DriverType.OUTPUT,
        Seq.empty[SubPlanInfo.DriverOption],
        outputOperator))

    val foosInput = subplan.findIn(foosMarker)

    implicit val context = newNodeCompilerContext(flowId, classServer.root.toFile)
    context.branchKeys.getField(foosInput.getOperator.getSerialNumber)

    val compiler = NodeCompiler.get(subplan)
    val thisType = compiler.compile(subplan)
    context.addClass(context.branchKeys)
    context.addClass(context.broadcastIds)
    val cls = classServer.loadClass(thisType).asSubclass(classOf[DirectOutputPrepare[Foo]])

    val numSlices = 2
    val files = (0 until 10).map(i => new File(root, s"group_bar/foo_bar_${i}.bin"))

    implicit val jobContext = newJobContext(sc)

    val source =
      new ParallelCollectionSource(Input,
        (0 until 100).map(i => (i, math.random)).sortBy(_._2).map(_._1), Some(numSlices))("input")
        .map(Input)(Foo.intToFoo)

    val setup = new Setup("setup")
    val prepare = cls.getConstructor(
      classOf[Action[Unit]],
      classOf[Seq[(Source, BranchKey)]],
      classOf[Partitioner],
      classOf[JobContext])
      .newInstance(
        setup,
        Seq((source, Input)),
        new HashPartitioner(sc.defaultParallelism),
        jobContext)
    val commit = new Commit(prepare)("test/group")

    val rc = newRoundContext(flowId = flowId, batchArguments = Map("arg" -> "bar"))

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
    val statistics = jobContext.outputStatistics(Direct)(outputOperator.getName)
    assert(statistics.files === 10)
    assert(statistics.bytes === 3150)
    assert(statistics.records === 100)
  }
}

object DirectOutputPrepareClassBuilderSpec {

  class Setup(
    val label: String)(
      implicit val jobContext: JobContext)
    extends Action[Unit] with runtime.CacheOnce[RoundContext, Future[Unit]] {

    override protected def doPerform(
      rc: RoundContext)(implicit ec: ExecutionContext): Future[Unit] = Future {}
  }

  class Commit(
    prepares: Set[Action[Unit]])(
      val basePaths: Set[String])(
        implicit jobContext: JobContext)
    extends DirectOutputCommit(prepares) with runtime.CacheOnce[RoundContext, Future[Unit]] {

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

    def getIdOption: IntOption = id
    def getGroupOption: IntOption = group

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
