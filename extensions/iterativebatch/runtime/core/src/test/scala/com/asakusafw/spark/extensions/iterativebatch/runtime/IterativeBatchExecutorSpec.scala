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
package com.asakusafw.spark.extensions.iterativebatch.runtime

import org.junit.runner.RunWith
import org.scalatest.fixture.FlatSpec
import org.scalatest.junit.JUnitRunner

import java.util.concurrent.{ ArrayBlockingQueue, BlockingQueue, TimeUnit }

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.concurrent.{ Await, ExecutionContext, Future }
import scala.util.Try

import org.apache.spark.{ SparkConf, SparkContext }

import com.asakusafw.bridge.stage.StageInfo
import com.asakusafw.spark.runtime.{ RoundContext, RoundContextSugar }
import com.asakusafw.spark.runtime.fixture.SparkForAll
import com.asakusafw.spark.runtime.graph.{
  Job,
  Node,
  Sink,
  Source
}
import com.asakusafw.spark.runtime.rdd.BranchKey

import com.asakusafw.spark.extensions.iterativebatch.runtime.graph.RoundAwareParallelCollectionSource
import com.asakusafw.spark.extensions.iterativebatch.runtime.util.ReadWriteLockedMap

@RunWith(classOf[JUnitRunner])
class IterativeBatchExecutorSpecTest extends IterativeBatchExecutorSpec

class IterativeBatchExecutorSpec extends FlatSpec with SparkForAll with RoundContextSugar {

  import IterativeBatchExecutorSpec._

  behavior of classOf[IterativeBatchExecutor].getSimpleName

  it should "handle RoundContexts" in { implicit sc =>
    import Simple._

    val rcs = (0 until 10).map { round =>
      newRoundContext(batchArguments = Map("round" -> round.toString))
    }

    val collection =
      new mutable.HashMap[RoundContext, Array[Int]] with ReadWriteLockedMap[RoundContext, Array[Int]]

    val executor = new Executor(collection)(implicitly, ExecutionContext.global)
    assert(executor.running === false)
    assert(executor.terminating === false)
    assert(executor.stopped === false)

    executor.submitAll(rcs)

    intercept[IllegalStateException] {
      executor.awaitExecution()
    }
    intercept[IllegalStateException] {
      executor.awaitTermination()
    }

    executor.start()
    assert(executor.running === true)
    assert(executor.terminating === false)
    assert(executor.stopped === false)

    val remain = executor.stop(awaitExecution = true, gracefully = true)
    assert(executor.running === false)
    assert(executor.terminating === false)
    assert(executor.stopped === true)
    assert(executor.queueSize === 0)
    assert(executor.numRunningBatches === 0)
    assert(remain.isEmpty)

    rcs.zipWithIndex.foreach {
      case (rc, round) =>
        assert(executor.result(rc).get.isSuccess)

        val result = collection(rc)
        assert(result === (0 until 100).map(i => 100 * round + i))
    }

    intercept[IllegalStateException] {
      executor.submit(newRoundContext())
    }
    intercept[IllegalStateException] {
      executor.awaitExecution()
    }
    intercept[IllegalStateException] {
      executor.awaitTermination()
    }
  }

  it should "handle `awaitExecution()`" in { implicit sc =>
    import Long._

    val rcs = (0 until 10).map { round =>
      newRoundContext(batchArguments = Map("round" -> round.toString))
    }
    val (rcs1, rcs2) = rcs.splitAt(5)

    val collection =
      new mutable.HashMap[RoundContext, Array[Int]] with ReadWriteLockedMap[RoundContext, Array[Int]]

    val executor = new Executor(10L, collection)(implicitly, ExecutionContext.global)
    try {
      executor.start()

      executor.submitAll(rcs1)

      assert(executor.awaitExecution(1, TimeUnit.MILLISECONDS) === false)

      assert(executor.awaitExecution(1, TimeUnit.MINUTES) === true)
      assert(executor.queueSize === 0)
      assert(executor.numRunningBatches === 0)

      executor.submitAll(rcs2)

      executor.awaitExecution()
      assert(executor.queueSize === 0)
      assert(executor.numRunningBatches === 0)

    } finally {
      executor.stop()
    }

    rcs.zipWithIndex.foreach {
      case (rc, round) =>
        assert(executor.result(rc).get.isSuccess)

        val result = collection(rc)
        assert(result === (0 until 10).map(i => 10 * round + i))
    }
  }

  it should "handle event listener" in { implicit sc =>
    import Simple._

    val rcs = (0 until 10).map { round =>
      newRoundContext(batchArguments = Map("round" -> round.toString))
    }

    val collection =
      new mutable.HashMap[RoundContext, Array[Int]] with ReadWriteLockedMap[RoundContext, Array[Int]]

    val listener = new CallCountListener()

    val executor = new Executor(collection)(implicitly, ExecutionContext.global)
    executor.addListener(listener)
    executor.submitAll(rcs)

    executor.start()
    executor.stop(awaitExecution = true, gracefully = true)

    assert(listener.callOnExecutorStart === 1)
    assert(listener.callOnRoundSubmitted === 10)
    assert(listener.callOnRoundStart === 10)
    assert(listener.callOnRoundCompleted === 10)
    assert(listener.callOnExecutorStop === 1)

    assert(listener.roundSuccess === 10)
    assert(listener.roundFailure === 0)

    rcs.zipWithIndex.foreach {
      case (rc, round) =>
        assert(executor.result(rc).get.isSuccess)

        val result = collection(rc)
        assert(result === (0 until 100).map(i => 100 * round + i))
    }
  }

  it should "handle exception to stop on fail" in { implicit sc =>
    import Exception._

    val rcs = (0 until 10).map { round =>
      newRoundContext(batchArguments = Map("round" -> round.toString))
    }

    val collection =
      new mutable.HashMap[RoundContext, Array[Int]] with ReadWriteLockedMap[RoundContext, Array[Int]]

    val listener = new CallCountListener()

    val maxRounds = 8
    val executor = new Executor(maxRounds, collection, stopOnFail = true)(implicitly, ExecutionContext.global)
    executor.addListener(listener)
    executor.submitAll(rcs)

    executor.start()
    val remain = executor.stop(awaitExecution = true, gracefully = true)

    assert(listener.callOnExecutorStart === 1)
    assert(listener.callOnRoundSubmitted === 10)
    assert(listener.callOnRoundStart === maxRounds + 1)
    assert(listener.callOnRoundCompleted === maxRounds + 1)
    assert(listener.callOnExecutorStop === 1)

    assert(listener.roundSuccess === maxRounds)
    assert(listener.roundFailure === 1)

    assert(remain.size === 1)

    rcs.zipWithIndex.foreach {
      case (rc, round) if round < maxRounds =>
        assert(executor.result(rc).get.isSuccess)

        val result = collection(rc)
        assert(result === (0 until 10).map(i => 10 * round + i))

      case (rc, round) if round == maxRounds =>
        assert(executor.result(rc).get.isFailure)
      case (rc, _) =>
        assert(executor.result(rc).isEmpty)
    }
  }

  it should "handle exception not to stop on fail" in { implicit sc =>
    import Exception._

    val rcs = (0 until 10).map { round =>
      newRoundContext(batchArguments = Map("round" -> round.toString))
    }

    val collection =
      new mutable.HashMap[RoundContext, Array[Int]] with ReadWriteLockedMap[RoundContext, Array[Int]]

    val listener = new CallCountListener()

    val maxRounds = 8
    val executor = new Executor(maxRounds, collection, stopOnFail = false)(implicitly, ExecutionContext.global)
    executor.addListener(listener)
    executor.submitAll(rcs)

    executor.start()
    val remain = executor.stop(awaitExecution = true, gracefully = true)

    assert(listener.callOnExecutorStart === 1)
    assert(listener.callOnRoundSubmitted === 10)
    assert(listener.callOnRoundStart === 10)
    assert(listener.callOnRoundCompleted === 10)
    assert(listener.callOnExecutorStop === 1)

    assert(listener.roundSuccess === maxRounds)
    assert(listener.roundFailure === 2)

    assert(remain.size === 0)

    rcs.zipWithIndex.foreach {
      case (rc, round) if round < maxRounds =>
        assert(executor.result(rc).get.isSuccess)

        val result = collection(rc)
        assert(result === (0 until 10).map(i => 10 * round + i))

      case (rc, _) =>
        assert(executor.result(rc).get.isFailure)
    }
  }
}

object IterativeBatchExecutorSpec {

  val Branch = BranchKey(0)

  class PrintSink(prev: Source)(implicit val sc: SparkContext) extends Sink {

    override val label: String = "print"

    override def submitJob(rc: RoundContext)(implicit ec: ExecutionContext): Future[Unit] = {
      prev.getOrCompute(rc).apply(Branch).map {
        _().foreach(println)
      }
    }
  }

  class CollectSink(
    collection: mutable.Map[RoundContext, Array[Int]])(
      prev: Source)(implicit val sc: SparkContext) extends Sink {

    override val label: String = "collect"

    override def submitJob(rc: RoundContext)(implicit ec: ExecutionContext): Future[Unit] = {
      prev.getOrCompute(rc).apply(Branch).map { rdd =>
        collection +=
          rc -> rdd().map {
            case i: Int => i
          }.collect()
      }
    }
  }

  object Simple {

    class Executor(
      slots: Int,
      collection: mutable.Map[RoundContext, Array[Int]])(
        implicit sc: SparkContext, ec: ExecutionContext)
      extends IterativeBatchExecutor(slots) {

      def this(collection: mutable.Map[RoundContext, Array[Int]])(
        implicit sc: SparkContext, ec: ExecutionContext) =
        this(Int.MaxValue, collection)

      override val job: Job = {
        val source = new RoundAwareParallelCollectionSource(Branch, (0 until 100))("source")
          .mapWithRoundContext(Branch) { rc =>

            val stageInfo = StageInfo.deserialize(rc.hadoopConf.value.get(StageInfo.KEY_NAME))
            val round = stageInfo.getBatchArguments()("round").toInt

            { i: Int => 100 * round + i }
          }
        new Job(Seq(source, new PrintSink(source), new CollectSink(collection)(source)))
      }
    }
  }

  object Long {

    class Executor(
      duration: Long,
      slots: Int,
      collection: mutable.Map[RoundContext, Array[Int]])(
        implicit sc: SparkContext, ec: ExecutionContext)
      extends IterativeBatchExecutor(slots) {

      def this(duration: Long, collection: mutable.Map[RoundContext, Array[Int]])(
        implicit sc: SparkContext, ec: ExecutionContext) =
        this(duration, Int.MaxValue, collection)

      override val job: Job = {
        val d = duration
        val source = new RoundAwareParallelCollectionSource(Branch, (0 until 10))("source")
          .mapWithRoundContext(Branch) { rc =>

            val stageInfo = StageInfo.deserialize(rc.hadoopConf.value.get(StageInfo.KEY_NAME))
            val round = stageInfo.getBatchArguments()("round").toInt

            { i: Int => 10 * round + i }
          }
          .map(Branch) { i: Int =>
            Thread.sleep(d)
            i
          }
        new Job(Seq(source, new PrintSink(source), new CollectSink(collection)(source)))
      }
    }
  }

  object Exception {

    class Executor(
      maxRounds: Int,
      collection: mutable.Map[RoundContext, Array[Int]],
      stopOnFail: Boolean)(
        implicit sc: SparkContext, ec: ExecutionContext)
      extends IterativeBatchExecutor(numSlots = 1, stopOnFail = stopOnFail) {

      override val job: Job = {
        val max = maxRounds
        val source = new RoundAwareParallelCollectionSource(Branch, (0 until 10))("source")
          .mapWithRoundContext(Branch) { rc =>

            val stageInfo = StageInfo.deserialize(rc.hadoopConf.value.get(StageInfo.KEY_NAME))
            val round = stageInfo.getBatchArguments()("round").toInt

            { i: Int => 10 * round + i }
          }
          .map(Branch) { i: Int =>
            if (i >= 10 * max) {
              throw new Exception(s"The number of rounds should be less than ${max}: [${i / 10}].")
            }
            i
          }
        new Job(Seq(source, new PrintSink(source), new CollectSink(collection)(source)))
      }
    }
  }

  class CallCountListener extends IterativeBatchExecutor.Listener {

    var callOnExecutorStart = 0
    var callOnRoundSubmitted = 0
    var callOnRoundStart = 0
    var callOnRoundCompleted = 0
    var callOnExecutorStop = 0

    var roundSuccess = 0
    var roundFailure = 0

    override def onExecutorStart(): Unit = {
      println("ExecutorStart")
      callOnExecutorStart += 1
    }

    override def onRoundSubmitted(rc: RoundContext): Unit = {
      println(s"RoundSubmitted: ${rc}")
      callOnRoundSubmitted += 1
    }

    override def onRoundStart(rc: RoundContext): Unit = {
      println(s"RoundStart: ${rc}")
      callOnRoundStart += 1
    }

    override def onRoundCompleted(rc: RoundContext, result: Try[Unit]): Unit = {
      println(s"RoundCompleted: ${rc}, ${result}")
      callOnRoundCompleted += 1
      if (result.isSuccess) {
        roundSuccess += 1
      } else {
        roundFailure += 1
      }
    }

    override def onExecutorStop(): Unit = {
      println("ExecutorStop")
      callOnExecutorStop += 1
    }
  }
}
