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
package graph

import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner

import java.io.{ DataInput, DataOutput, File }

import scala.collection.JavaConversions._
import scala.concurrent.{ Await, ExecutionContext, Future }
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.reflect.{ classTag, ClassTag }

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{ NullWritable, Writable }
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat
import org.apache.spark.{ HashPartitioner, Partitioner, SparkConf }
import org.apache.spark.rdd.RDD

import com.asakusafw.bridge.stage.StageInfo
import com.asakusafw.lang.compiler.api.CompilerOptions
import com.asakusafw.lang.compiler.api.testing.MockJobflowProcessorContext
import com.asakusafw.lang.compiler.extension.directio.{
  DirectFileIoConstants,
  DirectFileOutputModel
}
import com.asakusafw.lang.compiler.model.description.{ ClassDescription, Descriptions }
import com.asakusafw.lang.compiler.model.graph.{ ExternalOutput, MarkerOperator }
import com.asakusafw.lang.compiler.model.info.ExternalOutputInfo
import com.asakusafw.lang.compiler.planning.{ PlanBuilder, PlanMarker, SubPlan }
import com.asakusafw.runtime.directio.DataFormat
import com.asakusafw.runtime.directio.hadoop.{ HadoopDataSource, SequenceFileFormat }
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.runtime.value.{ IntOption, StringOption, ValueOption }
import com.asakusafw.spark.compiler._
import com.asakusafw.spark.compiler.graph._
import com.asakusafw.spark.compiler.directio.DirectOutputDescription
import com.asakusafw.spark.compiler.planning.{ IterativeInfo, SubPlanInfo }
import com.asakusafw.spark.compiler.spi.NodeCompiler
import com.asakusafw.spark.runtime._
import com.asakusafw.spark.runtime.JobContext.OutputCounter.Direct
import com.asakusafw.spark.runtime.directio.OutputPatternGenerator
import com.asakusafw.spark.runtime.directio.OutputPatternGenerator._
import com.asakusafw.spark.runtime.graph._
import com.asakusafw.spark.runtime.rdd.{ BranchKey, IdentityPartitioner, ShuffleKey }

import com.asakusafw.spark.extensions.iterativebatch.runtime.{ graph => runtime }
import com.asakusafw.spark.extensions.iterativebatch.runtime.graph._
import com.asakusafw.spark.extensions.iterativebatch.compiler.spi.RoundAwareNodeCompiler

@RunWith(classOf[JUnitRunner])
class DirectOutputPrepareForIterativeClassBuilderSpecTest
  extends DirectOutputPrepareForIterativeClassBuilderSpec

class DirectOutputPrepareForIterativeClassBuilderSpec
  extends FlatSpec
  with ClassServerForAll
  with SparkForAll
  with FlowIdForEach
  with UsingCompilerContext
  with JobContextSugar
  with RoundContextSugar
  with TempDirForAll {

  import DirectOutputPrepareForIterativeClassBuilderSpec._

  behavior of classOf[DirectOutputPrepareForIterativeClassBuilder].getSimpleName

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

  def newPrepareFlatEach(
    subplan: SubPlan)(
      prevs: Seq[(Source, BranchKey)])(
        implicit context: MockCompilerContext.NodeCompiler,
        jobContext: JobContext): DirectOutputPrepareFlatEachForIterative[Foo] = {

    val compiler = RoundAwareNodeCompiler.get(subplan)
    val thisType = compiler.compile(subplan)
    context.addClass(context.branchKeys)
    context.addClass(context.broadcastIds)
    val cls = classServer.loadClass(thisType)
      .asSubclass(classOf[DirectOutputPrepareFlatEachForIterative[Foo]])
    cls.getConstructor(classOf[Seq[_]], classOf[JobContext])
      .newInstance(prevs, jobContext)
  }

  def newPrepareGroupEach(
    subplan: SubPlan)(
      prevs: Seq[(Source, BranchKey)])(
        partitioner: Partitioner)(
          implicit context: MockCompilerContext.NodeCompiler,
          jobContext: JobContext): DirectOutputPrepareGroupEachForIterative[Foo] = {

    val compiler = RoundAwareNodeCompiler.get(subplan)
    val thisType = compiler.compile(subplan)
    context.addClass(context.branchKeys)
    context.addClass(context.broadcastIds)
    val cls = classServer.loadClass(thisType)
      .asSubclass(classOf[DirectOutputPrepareGroupEachForIterative[Foo]])
    cls.getConstructor(classOf[Seq[_]], classOf[Partitioner], classOf[JobContext])
      .newInstance(prevs, partitioner, jobContext)
  }

  def newPrepare(
    subplan: SubPlan)(
      setup: IterativeAction[Unit],
      prepare: DirectOutputPrepareEachForIterative[Foo])(
        implicit context: MockCompilerContext.NodeCompiler,
        jobContext: JobContext): DirectOutputPrepareForIterative[Foo] = {
    val thisType = DirectOutputPrepareForIterativeCompiler.compile(subplan)
    val cls = classServer.loadClass(thisType)
      .asSubclass(classOf[DirectOutputPrepareForIterative[Foo]])
    cls.getConstructor(
      classOf[IterativeAction[Unit]],
      classOf[DirectOutputPrepareEachForIterative[_]],
      classOf[JobContext])
      .newInstance(setup, prepare, jobContext)
  }

  {
    var idx = 0
    var stageOffset = 0

    for {
      iterativeInfo <- Seq(
        IterativeInfo.always(),
        IterativeInfo.never(),
        IterativeInfo.parameter("round"))
    } {
      val conf = s"IterativeInfo: ${iterativeInfo}"

      it should s"prepare flat simple: [${conf}]" in {
        val foosMarker = MarkerOperator.builder(ClassDescription.of(classOf[Foo]))
          .attribute(classOf[PlanMarker], PlanMarker.GATHER).build()
        val endMarker = MarkerOperator.builder(ClassDescription.of(classOf[Foo]))
          .attribute(classOf[PlanMarker], PlanMarker.END).build()

        val outputOperator = ExternalOutput.builder(
          s"flat${idx}",
          new ExternalOutputInfo.Basic(
            ClassDescription.of(classOf[DirectFileOutputModel]),
            DirectFileIoConstants.MODULE_NAME,
            ClassDescription.of(classOf[Foo]),
            Descriptions.valueOf(
              new DirectFileOutputModel(
                DirectOutputDescription(
                  basePath = s"test/flat${idx}_$${round}",
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
        subplan.putAttr(_ => iterativeInfo)

        val foosInput = subplan.findIn(foosMarker)

        implicit val context = newNodeCompilerContext(flowId, classServer.root.toFile)
        context.branchKeys.getField(foosInput.getOperator.getSerialNumber)

        val rounds = 0 to 1
        val numSlices = 2
        val files = rounds.map { round =>
          (0 until numSlices).map(i => new File(root, s"flat${idx}_${round}/s${round + stageOffset}-p${i}.bin"))
        }

        implicit val jobContext = newJobContext(sc)

        val source =
          new RoundAwareParallelCollectionSource(Input, 0 until 100, Some(numSlices))("input")
            .mapWithRoundContext(Input)(Foo.intToFoo)

        val setup = new Setup("setup")
        val prepareEach = newPrepareFlatEach(subplan)(Seq((source, Input)))
        val prepare = newPrepare(subplan)(setup, prepareEach)
        val commit = new Commit(prepare)(s"test/flat${idx}_$${round}")

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
        if (iterativeInfo.getRecomputeKind != IterativeInfo.RecomputeKind.NEVER) {
          assert(files.forall(_.forall(_.exists())) === true)

          rounds.foreach { round =>
            assert(
              sc.newAPIHadoopFile[NullWritable, Foo, SequenceFileInputFormat[NullWritable, Foo]](
                new File(root, s"flat${idx}_${round}/s${round + stageOffset}-*.bin").getAbsolutePath)
                .map(_._2)
                .map(foo => (foo.id.get, foo.group.get))
                .collect.toSeq
                === (0 until 100).map(i => (100 * round + i, i % 10)))
          }

          assert(jobContext.outputStatistics(Direct).size === 1)
          val statistics = jobContext.outputStatistics(Direct)(outputOperator.getName)
          assert(statistics.files === 4)
          assert(statistics.bytes === 4292)
          assert(statistics.records === 200)

          stageOffset += 7
        } else {
          assert(files.head.forall(_.exists()) === true)
          assert(files.tail.exists(_.exists(_.exists())) === false)

          val round = rounds.head
          assert(
            sc.newAPIHadoopFile[NullWritable, Foo, SequenceFileInputFormat[NullWritable, Foo]](
              new File(root, s"flat${idx}_${round}/s${round + stageOffset}-*.bin").getAbsolutePath)
              .map(_._2)
              .map(foo => (foo.id.get, foo.group.get))
              .collect.toSeq
              === (0 until 100).map(i => (100 * round + i, i % 10)))

          assert(jobContext.outputStatistics(Direct).size === 1)
          val statistics = jobContext.outputStatistics(Direct)(outputOperator.getName)
          assert(statistics.files === 2)
          assert(statistics.bytes === 2146)
          assert(statistics.records === 100)

          stageOffset += 4
        }
        idx += 1
      }

      it should s"prepare flat simple with batch argument: [${conf}]" in {
        val foosMarker = MarkerOperator.builder(ClassDescription.of(classOf[Foo]))
          .attribute(classOf[PlanMarker], PlanMarker.GATHER).build()
        val endMarker = MarkerOperator.builder(ClassDescription.of(classOf[Foo]))
          .attribute(classOf[PlanMarker], PlanMarker.END).build()

        val outputOperator = ExternalOutput.builder(
          s"flat${idx}",
          new ExternalOutputInfo.Basic(
            ClassDescription.of(classOf[DirectFileOutputModel]),
            DirectFileIoConstants.MODULE_NAME,
            ClassDescription.of(classOf[Foo]),
            Descriptions.valueOf(
              new DirectFileOutputModel(
                DirectOutputDescription(
                  basePath = s"test/flat${idx}_$${arg}_$${round}",
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
        subplan.putAttr(_ => iterativeInfo)

        val foosInput = subplan.findIn(foosMarker)

        implicit val context = newNodeCompilerContext(flowId, classServer.root.toFile)
        context.branchKeys.getField(foosInput.getOperator.getSerialNumber)

        val rounds = 0 to 1
        val numSlices = 2
        val files = rounds.map { round =>
          (0 until numSlices).map(i => new File(root, s"flat${idx}_bar_${round}/bar_s${round + stageOffset}-p${i}.bin"))
        }

        implicit val jobContext = newJobContext(sc)

        val source =
          new RoundAwareParallelCollectionSource(Input, 0 until 100, Some(numSlices))("input")
            .mapWithRoundContext(Input)(Foo.intToFoo)

        val setup = new Setup("setup")
        val prepareEach = newPrepareFlatEach(subplan)(Seq((source, Input)))
        val prepare = newPrepare(subplan)(setup, prepareEach)
        val commit = new Commit(prepare)(s"test/flat${idx}_$${round}")

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
        if (iterativeInfo.getRecomputeKind != IterativeInfo.RecomputeKind.NEVER) {
          assert(files.forall(_.forall(_.exists())) === true)

          rounds.foreach { round =>
            assert(
              sc.newAPIHadoopFile[NullWritable, Foo, SequenceFileInputFormat[NullWritable, Foo]](
                new File(root, s"flat${idx}_bar_${round}/bar_s${round + stageOffset}-*.bin").getAbsolutePath)
                .map(_._2)
                .map(foo => (foo.id.get, foo.group.get))
                .collect.toSeq
                === (0 until 100).map(i => (100 * round + i, i % 10)))
          }

          assert(jobContext.outputStatistics(Direct).size === 1)
          val statistics = jobContext.outputStatistics(Direct)(outputOperator.getName)
          assert(statistics.files === 4)
          assert(statistics.bytes === 4292)
          assert(statistics.records === 200)

          stageOffset += 7
        } else {
          assert(files.head.forall(_.exists()) === true)
          assert(files.tail.exists(_.exists(_.exists())) === false)

          val round = rounds.head
          assert(
            sc.newAPIHadoopFile[NullWritable, Foo, SequenceFileInputFormat[NullWritable, Foo]](
              new File(root, s"flat${idx}_bar_${round}/bar_s${round + stageOffset}-*.bin").getAbsolutePath)
              .map(_._2)
              .map(foo => (foo.id.get, foo.group.get))
              .collect.toSeq
              === (0 until 100).map(i => (100 * round + i, i % 10)))

          assert(jobContext.outputStatistics(Direct).size === 1)
          val statistics = jobContext.outputStatistics(Direct)(outputOperator.getName)
          assert(statistics.files === 2)
          assert(statistics.bytes === 2146)
          assert(statistics.records === 100)

          stageOffset += 4
        }
        idx += 1
      }

      it should s"prepare flat w/o round variable: [${conf}]" in {
        val foosMarker = MarkerOperator.builder(ClassDescription.of(classOf[Foo]))
          .attribute(classOf[PlanMarker], PlanMarker.GATHER).build()
        val endMarker = MarkerOperator.builder(ClassDescription.of(classOf[Foo]))
          .attribute(classOf[PlanMarker], PlanMarker.END).build()

        val outputOperator = ExternalOutput.builder(
          s"flat${idx}",
          new ExternalOutputInfo.Basic(
            ClassDescription.of(classOf[DirectFileOutputModel]),
            DirectFileIoConstants.MODULE_NAME,
            ClassDescription.of(classOf[Foo]),
            Descriptions.valueOf(
              new DirectFileOutputModel(
                DirectOutputDescription(
                  basePath = s"test/flat${idx}",
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
        subplan.putAttr(_ => iterativeInfo)

        val foosInput = subplan.findIn(foosMarker)

        implicit val context = newNodeCompilerContext(flowId, classServer.root.toFile)
        context.branchKeys.getField(foosInput.getOperator.getSerialNumber)

        val rounds = 0 to 1
        val numSlices = 2
        val files = rounds.map { round =>
          (0 until numSlices).map(i => new File(root, s"flat${idx}/s${round + stageOffset}-p${i}.bin"))
        }

        implicit val jobContext = newJobContext(sc)

        val source =
          new RoundAwareParallelCollectionSource(Input, 0 until 100, Some(numSlices))("input")
            .mapWithRoundContext(Input)(Foo.intToFoo)

        val setup = new Setup("setup")
        val prepareEach = newPrepareFlatEach(subplan)(Seq((source, Input)))
        val prepare = newPrepare(subplan)(setup, prepareEach)
        val commit = new Commit(prepare)(s"test/flat${idx}")

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
        if (iterativeInfo.getRecomputeKind != IterativeInfo.RecomputeKind.NEVER) {
          assert(files.forall(_.forall(_.exists())) === true)

          rounds.foreach { round =>
            assert(
              sc.newAPIHadoopFile[NullWritable, Foo, SequenceFileInputFormat[NullWritable, Foo]](
                new File(root, s"flat${idx}/s${round + stageOffset}-*.bin").getAbsolutePath)
                .map(_._2)
                .map(foo => (foo.id.get, foo.group.get))
                .collect.toSeq
                === (0 until 100).map(i => (100 * round + i, i % 10)))
          }

          assert(jobContext.outputStatistics(Direct).size === 1)
          val statistics = jobContext.outputStatistics(Direct)(outputOperator.getName)
          assert(statistics.files === 4)
          assert(statistics.bytes === 4292)
          assert(statistics.records === 200)

          stageOffset += 7
        } else {
          assert(files.head.forall(_.exists()) === true)
          assert(files.tail.exists(_.exists(_.exists())) === false)

          val round = rounds.head
          assert(
            sc.newAPIHadoopFile[NullWritable, Foo, SequenceFileInputFormat[NullWritable, Foo]](
              new File(root, s"flat${idx}/s${round + stageOffset}-*.bin").getAbsolutePath)
              .map(_._2)
              .map(foo => (foo.id.get, foo.group.get))
              .collect.toSeq
              === (0 until 100).map(i => (100 * round + i, i % 10)))

          assert(jobContext.outputStatistics(Direct).size === 1)
          val statistics = jobContext.outputStatistics(Direct)(outputOperator.getName)
          assert(statistics.files === 2)
          assert(statistics.bytes === 2146)
          assert(statistics.records === 100)

          stageOffset += 4
        }
        idx += 1
      }

      it should s"prepare flat a part of rounds: [${conf}]" in {
        val foosMarker = MarkerOperator.builder(ClassDescription.of(classOf[Foo]))
          .attribute(classOf[PlanMarker], PlanMarker.GATHER).build()
        val endMarker = MarkerOperator.builder(ClassDescription.of(classOf[Foo]))
          .attribute(classOf[PlanMarker], PlanMarker.END).build()

        val outputOperator = ExternalOutput.builder(
          s"flat${idx}",
          new ExternalOutputInfo.Basic(
            ClassDescription.of(classOf[DirectFileOutputModel]),
            DirectFileIoConstants.MODULE_NAME,
            ClassDescription.of(classOf[Foo]),
            Descriptions.valueOf(
              new DirectFileOutputModel(
                DirectOutputDescription(
                  basePath = s"test/flat${idx}_$${round}",
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
        subplan.putAttr(_ => iterativeInfo)

        val foosInput = subplan.findIn(foosMarker)

        implicit val context = newNodeCompilerContext(flowId, classServer.root.toFile)
        context.branchKeys.getField(foosInput.getOperator.getSerialNumber)

        val rounds = 0 to 1
        val numSlices = 2
        val files = rounds.map { round =>
          (0 until numSlices).map(i => new File(root, s"flat${idx}_${round}/s${round + stageOffset}-p${i}.bin"))
        }

        implicit val jobContext = newJobContext(sc)

        val source =
          new RoundAwareParallelCollectionSource(Input, 0 until 100, Some(numSlices))("input")
            .mapWithRoundContext(Input)(Foo.intToFoo)

        val setup = new Setup("setup")
        val prepareEach = newPrepareFlatEach(subplan)(Seq((source, Input)))
        val prepare = newPrepare(subplan)(setup, prepareEach)
        val commit = new Commit(prepare)(s"test/flat${idx}_$${round}")

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

        val round = rounds.head
        assert(
          sc.newAPIHadoopFile[NullWritable, Foo, SequenceFileInputFormat[NullWritable, Foo]](
            new File(root, s"flat${idx}_${round}/s${round + stageOffset}-*.bin").getAbsolutePath)
            .map(_._2)
            .map(foo => (foo.id.get, foo.group.get))
            .collect.toSeq
            === (0 until 100).map(i => (100 * round + i, i % 10)))

        assert(jobContext.outputStatistics(Direct).size === 1)
        val statistics = jobContext.outputStatistics(Direct)(outputOperator.getName)
        assert(statistics.files === 2)
        assert(statistics.bytes === 2146)
        assert(statistics.records === 100)

        if (iterativeInfo.getRecomputeKind != IterativeInfo.RecomputeKind.NEVER) {
          stageOffset += 5
        } else {
          stageOffset += 4
        }
        idx += 1
      }
    }
  }

  {
    var idx = 0

    for {
      iterativeInfo <- Seq(
        IterativeInfo.always(),
        IterativeInfo.never(),
        IterativeInfo.parameter("round"))
    } {
      val conf = s"IterativeInfo: ${iterativeInfo}"

      it should s"prepare group simple: [${conf}]" in {
        val foosMarker = MarkerOperator.builder(ClassDescription.of(classOf[Foo]))
          .attribute(classOf[PlanMarker], PlanMarker.GATHER).build()
        val endMarker = MarkerOperator.builder(ClassDescription.of(classOf[Foo]))
          .attribute(classOf[PlanMarker], PlanMarker.END).build()

        val outputOperator = ExternalOutput.builder(
          s"group${idx}",
          new ExternalOutputInfo.Basic(
            ClassDescription.of(classOf[DirectFileOutputModel]),
            DirectFileIoConstants.MODULE_NAME,
            ClassDescription.of(classOf[Foo]),
            Descriptions.valueOf(
              new DirectFileOutputModel(
                DirectOutputDescription(
                  basePath = s"test/group${idx}_$${round}",
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
        subplan.putAttr(_ => iterativeInfo)

        val foosInput = subplan.findIn(foosMarker)

        implicit val context = newNodeCompilerContext(flowId, classServer.root.toFile)
        context.branchKeys.getField(foosInput.getOperator.getSerialNumber)

        val rounds = 0 to 1
        val numSlices = 2
        val files = rounds.map { round =>
          (0 until numSlices).map(i => new File(root, s"group${idx}_${round}/foo_${i}.bin"))
        }

        implicit val jobContext = newJobContext(sc)

        val source =
          new RoundAwareParallelCollectionSource(Input, 0 until 100, Some(numSlices))("input")
            .mapWithRoundContext(Input)(Foo.intToFoo)

        val setup = new Setup("setup")
        val prepareEach = newPrepareGroupEach(subplan)(
          Seq((source, Input)))(new HashPartitioner(sc.defaultParallelism))
        val prepare = newPrepare(subplan)(setup, prepareEach)
        val commit = new Commit(prepare)(s"test/group${idx}_$${round}")

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
        if (iterativeInfo.getRecomputeKind != IterativeInfo.RecomputeKind.NEVER) {
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
          val statistics = jobContext.outputStatistics(Direct)(outputOperator.getName)
          assert(statistics.files === 20)
          assert(statistics.bytes === 7060)
          assert(statistics.records === 200)

        } else {
          assert(files.head.forall(_.exists()) === true)
          assert(files.tail.exists(_.exists(_.exists())) === false)

          val round = rounds.head
          files.head.zipWithIndex.foreach {
            case (file, i) =>
              assert(
                sc.newAPIHadoopFile[NullWritable, Foo, SequenceFileInputFormat[NullWritable, Foo]](
                  file.getAbsolutePath)
                  .map(_._2)
                  .map(foo => (foo.id.get, foo.group.get))
                  .collect.toSeq
                  === (0 until 100).reverse.filter(_ % 10 == i).map(j => (100 * round + j, i)))
          }

          assert(jobContext.outputStatistics(Direct).size === 1)
          val statistics = jobContext.outputStatistics(Direct)(outputOperator.getName)
          assert(statistics.files === 10)
          assert(statistics.bytes === 3530)
          assert(statistics.records === 100)
        }
        idx += 1
      }

      it should s"prepare group simple with batch argument: [${conf}]" in {
        val foosMarker = MarkerOperator.builder(ClassDescription.of(classOf[Foo]))
          .attribute(classOf[PlanMarker], PlanMarker.GATHER).build()
        val endMarker = MarkerOperator.builder(ClassDescription.of(classOf[Foo]))
          .attribute(classOf[PlanMarker], PlanMarker.END).build()

        val outputOperator = ExternalOutput.builder(
          s"group${idx}",
          new ExternalOutputInfo.Basic(
            ClassDescription.of(classOf[DirectFileOutputModel]),
            DirectFileIoConstants.MODULE_NAME,
            ClassDescription.of(classOf[Foo]),
            Descriptions.valueOf(
              new DirectFileOutputModel(
                DirectOutputDescription(
                  basePath = s"test/group${idx}_$${arg}_$${round}",
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
        subplan.putAttr(_ => iterativeInfo)

        val foosInput = subplan.findIn(foosMarker)

        implicit val context = newNodeCompilerContext(flowId, classServer.root.toFile)
        context.branchKeys.getField(foosInput.getOperator.getSerialNumber)

        val rounds = 0 to 1
        val numSlices = 2
        val files = rounds.map { round =>
          (0 until numSlices).map(i => new File(root, s"group${idx}_bar_${round}/foo_bar_${i}.bin"))
        }

        implicit val jobContext = newJobContext(sc)

        val source =
          new RoundAwareParallelCollectionSource(Input, 0 until 100, Some(numSlices))("input")
            .mapWithRoundContext(Input)(Foo.intToFoo)

        val setup = new Setup("setup")
        val prepareEach = newPrepareGroupEach(subplan)(
          Seq((source, Input)))(new HashPartitioner(sc.defaultParallelism))
        val prepare = newPrepare(subplan)(setup, prepareEach)
        val commit = new Commit(prepare)(s"test/group${idx}_$${round}")

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
        if (iterativeInfo.getRecomputeKind != IterativeInfo.RecomputeKind.NEVER) {
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
          val statistics = jobContext.outputStatistics(Direct)(outputOperator.getName)
          assert(statistics.files === 20)
          assert(statistics.bytes === 7060)
          assert(statistics.records === 200)

        } else {
          assert(files.head.forall(_.exists()) === true)
          assert(files.tail.exists(_.exists(_.exists())) === false)

          val round = rounds.head
          files.head.zipWithIndex.foreach {
            case (file, i) =>
              assert(
                sc.newAPIHadoopFile[NullWritable, Foo, SequenceFileInputFormat[NullWritable, Foo]](
                  file.getAbsolutePath)
                  .map(_._2)
                  .map(foo => (foo.id.get, foo.group.get))
                  .collect.toSeq
                  === (0 until 100).reverse.filter(_ % 10 == i).map(j => (100 * round + j, i)))
          }

          assert(jobContext.outputStatistics(Direct).size === 1)
          val statistics = jobContext.outputStatistics(Direct)(outputOperator.getName)
          assert(statistics.files === 10)
          assert(statistics.bytes === 3530)
          assert(statistics.records === 100)
        }
        idx += 1
      }

      it should s"prepare group w/o round variable: [${conf}]" in {
        val foosMarker = MarkerOperator.builder(ClassDescription.of(classOf[Foo]))
          .attribute(classOf[PlanMarker], PlanMarker.GATHER).build()
        val endMarker = MarkerOperator.builder(ClassDescription.of(classOf[Foo]))
          .attribute(classOf[PlanMarker], PlanMarker.END).build()

        val outputOperator = ExternalOutput.builder(
          s"group${idx}",
          new ExternalOutputInfo.Basic(
            ClassDescription.of(classOf[DirectFileOutputModel]),
            DirectFileIoConstants.MODULE_NAME,
            ClassDescription.of(classOf[Foo]),
            Descriptions.valueOf(
              new DirectFileOutputModel(
                DirectOutputDescription(
                  basePath = s"test/group${idx}",
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
        subplan.putAttr(_ => iterativeInfo)

        val foosInput = subplan.findIn(foosMarker)

        implicit val context = newNodeCompilerContext(flowId, classServer.root.toFile)
        context.branchKeys.getField(foosInput.getOperator.getSerialNumber)

        val rounds = 0 to 1
        val numSlices = 2
        val files = (0 until numSlices).map(i => new File(root, s"group${idx}/foo_${i}.bin"))

        implicit val jobContext = newJobContext(sc)

        val source =
          new RoundAwareParallelCollectionSource(Input, 0 until 100, Some(numSlices))("input")
            .mapWithRoundContext(Input)(Foo.intToFoo)

        val setup = new Setup("setup")
        val prepareEach = newPrepareGroupEach(subplan)(
          Seq((source, Input)))(new HashPartitioner(sc.defaultParallelism))
        val prepare = newPrepare(subplan)(setup, prepareEach)
        val commit = new Commit(prepare)(s"test/group${idx}")

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

        if (iterativeInfo.getRecomputeKind != IterativeInfo.RecomputeKind.NEVER) {
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
          val statistics = jobContext.outputStatistics(Direct)(outputOperator.getName)
          assert(statistics.files === 10)
          assert(statistics.bytes === 5330)
          assert(statistics.records === 200)

        } else {
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
          assert(statistics.bytes === 3530)
          assert(statistics.records === 100)
        }
        idx += 1
      }

      it should s"prepare group a part of rounds: [${conf}]" in {
        val foosMarker = MarkerOperator.builder(ClassDescription.of(classOf[Foo]))
          .attribute(classOf[PlanMarker], PlanMarker.GATHER).build()
        val endMarker = MarkerOperator.builder(ClassDescription.of(classOf[Foo]))
          .attribute(classOf[PlanMarker], PlanMarker.END).build()

        val outputOperator = ExternalOutput.builder(
          s"group${idx}",
          new ExternalOutputInfo.Basic(
            ClassDescription.of(classOf[DirectFileOutputModel]),
            DirectFileIoConstants.MODULE_NAME,
            ClassDescription.of(classOf[Foo]),
            Descriptions.valueOf(
              new DirectFileOutputModel(
                DirectOutputDescription(
                  basePath = s"test/group${idx}_$${round}",
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
        subplan.putAttr(_ => iterativeInfo)

        val foosInput = subplan.findIn(foosMarker)

        implicit val context = newNodeCompilerContext(flowId, classServer.root.toFile)
        context.branchKeys.getField(foosInput.getOperator.getSerialNumber)

        val rounds = 0 to 1
        val numSlices = 2
        val files = rounds.map { round =>
          (0 until numSlices).map(i => new File(root, s"group${idx}_${round}/foo_${i}.bin"))
        }

        implicit val jobContext = newJobContext(sc)

        val source =
          new RoundAwareParallelCollectionSource(Input, 0 until 100, Some(numSlices))("input")
            .mapWithRoundContext(Input)(Foo.intToFoo)

        val setup = new Setup("setup")
        val prepareEach = newPrepareGroupEach(subplan)(
          Seq((source, Input)))(new HashPartitioner(sc.defaultParallelism))
        val prepare = newPrepare(subplan)(setup, prepareEach)
        val commit = new Commit(prepare)(s"test/group${idx}_$${round}")

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

        val round = rounds.head
        files.head.zipWithIndex.foreach {
          case (file, i) =>
            assert(
              sc.newAPIHadoopFile[NullWritable, Foo, SequenceFileInputFormat[NullWritable, Foo]](
                file.getAbsolutePath)
                .map(_._2)
                .map(foo => (foo.id.get, foo.group.get))
                .collect.toSeq
                === (0 until 100).reverse.filter(_ % 10 == i).map(j => (100 * round + j, i)))
        }

        assert(jobContext.outputStatistics(Direct).size === 1)
        val statistics = jobContext.outputStatistics(Direct)(outputOperator.getName)
        assert(statistics.files === 10)
        assert(statistics.bytes === 3530)
        assert(statistics.records === 100)

        idx += 1
      }
    }
  }
}

object DirectOutputPrepareForIterativeClassBuilderSpec {

  class Setup(
    val label: String)(
      implicit val jobContext: JobContext)
    extends IterativeAction[Unit] with runtime.CacheAlways[Seq[RoundContext], Future[Unit]] {

    override protected def doPerform(
      origin: RoundContext,
      rcs: Seq[RoundContext])(implicit ec: ExecutionContext): Future[Unit] = Future {}
  }

  class Commit(
    prepares: Set[IterativeAction[Unit]])(
      val basePaths: Set[String])(
        implicit jobContext: JobContext)
    extends DirectOutputCommitForIterative(prepares)
    with runtime.CacheAlways[Seq[RoundContext], Future[Unit]] {

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

    def getIdOption: IntOption = id
    def getGroupOption: IntOption = group

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
}
