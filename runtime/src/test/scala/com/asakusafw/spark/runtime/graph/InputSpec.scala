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
package com.asakusafw.spark.runtime
package graph

import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner
import java.io.{ DataInput, DataOutput, File }

import scala.concurrent.{ Await, Future }
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.reflect.{ classTag, ClassTag }
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{ NullWritable, Writable }
import org.apache.hadoop.mapreduce.{ InputFormat, Job => MRJob }
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.{ FileOutputFormat, SequenceFileOutputFormat }
import org.apache.spark.{ Partitioner, SparkConf }
import org.apache.spark.broadcast.{ Broadcast => Broadcasted }
import org.apache.spark.rdd.RDD
import com.asakusafw.bridge.hadoop.directio.DirectFileInputFormat
import com.asakusafw.bridge.hadoop.temporary.TemporaryFileOutputFormat
import com.asakusafw.runtime.directio.hadoop.{ HadoopDataSource, SequenceFileFormat }
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.runtime.value.IntOption
import com.asakusafw.spark.runtime.JobContext.InputCounter
import com.asakusafw.spark.runtime.TempDirForEach
import com.asakusafw.spark.runtime.aggregation.Aggregation
import com.asakusafw.spark.runtime.fragment.{ Fragment, GenericOutputFragment, OutputFragment }
import com.asakusafw.spark.runtime.rdd.{ BranchKey, ShuffleKey }

abstract class InputSpec extends FlatSpec with SparkForAll {

  import InputSpec._

  def prepareRound(
    path: String,
    configurePath: (MRJob, String) => Unit)(
      ints: Seq[Int]): Unit = {
    val job = MRJob.getInstance(sc.hadoopConfiguration)
    job.setOutputKeyClass(classOf[NullWritable])
    job.setOutputValueClass(classOf[Foo])
    configurePath(job, path)
    val foos = sc.parallelize(ints).map(Foo.intToFoo)
    foos.saveAsNewAPIHadoopDataset(job.getConfiguration)
  }
}

@RunWith(classOf[JUnitRunner])
class TemporaryInputSpecTest extends TemporaryInputSpec

class TemporaryInputSpec
  extends InputSpec
  with JobContextSugar
  with RoundContextSugar
  with TempDirForEach {

  import InputSpec._

  behavior of classOf[TemporaryInput[_]].getSimpleName

  val configurePath: (MRJob, String) => Unit = { (job, path) =>
    job.setOutputFormatClass(classOf[TemporaryFileOutputFormat[Foo]])
    FileOutputFormat.setOutputPath(job, new Path(path))
  }

  for {
    numSlices <- Seq(None, Some(8), Some(4))
  } {
    val conf = s"numSlices = ${numSlices}"

    it should s"input: [${conf}]" in {
      implicit val jobContext = newJobContext(sc)

      val tmpDir = createTempDirectoryForEach("test-").toFile.getAbsolutePath
      val path = new File(tmpDir, "foos").getAbsolutePath

      val input = new Temporary.Input(path)("input", "input")

      prepareRound(path, configurePath)(0 until 100)

      val rc = newRoundContext()

      val result = Await.result(
        input.compute(rc).apply(Input).map {
          _().map {
            case (_, foo: Foo) => foo.id.get
          }.collect.toSeq.sorted
        }, Duration.Inf)

      assert(result === (0 until 100))

      assert(jobContext.inputStatistics(InputCounter.External).size === 1)
      val statistics = jobContext.inputStatistics(InputCounter.External)("input")
      assert(statistics.records === 100)
    }

    it should s"input from multiple paths: [${conf}]" in {
      implicit val jobContext = newJobContext(sc)

      val tmpDir = createTempDirectoryForEach("test-").toFile.getAbsolutePath
      val path1 = new File(tmpDir, "foos1").getAbsolutePath
      val path2 = new File(tmpDir, "foos2").getAbsolutePath

      val input = new Temporary.Input(Set(path1, path2))("input", "input")

      prepareRound(path1, configurePath)(0 until 50)
      prepareRound(path2, configurePath)(50 until 100)

      val rc = newRoundContext()

      val result = Await.result(
        input.compute(rc).apply(Input).map {
          _().map {
            case (_, foo: Foo) => foo.id.get
          }.collect.toSeq.sorted
        }, Duration.Inf)

      assert(result === (0 until 100))

      assert(jobContext.inputStatistics(InputCounter.External).size === 1)
      val statistics = jobContext.inputStatistics(InputCounter.External)("input")
      assert(statistics.records === 100)
    }
  }
}

@RunWith(classOf[JUnitRunner])
class TemporaryInputWithParallelismSpecTest extends TemporaryInputWithParallelismSpec

class TemporaryInputWithParallelismSpec extends TemporaryInputSpec {

  override def configure(conf: SparkConf): SparkConf = {
    conf.set("spark.default.parallelism", 8.toString)
  }
}

@RunWith(classOf[JUnitRunner])
class DirectInputSpecTest extends DirectInputSpec

class DirectInputSpec
  extends InputSpec
  with JobContextSugar
  with RoundContextSugar
  with TempDirForEach {

  import InputSpec._

  behavior of classOf[DirectInput[_, _, _]].getSimpleName

  val configurePath: (MRJob, String) => Unit = { (job, path) =>
    job.setOutputFormatClass(classOf[SequenceFileOutputFormat[NullWritable, Foo]])
    FileOutputFormat.setOutputPath(job, new Path(path))
  }

  for {
    numSlices <- Seq(None, Some(8), Some(4))
  } {
    val conf = s"numSlices = ${numSlices}"

    it should s"input: [${conf}]" in {
      implicit val jobContext = newJobContext(sc)

      val tmpDir = createTempDirectoryForEach("test-").toFile.getAbsolutePath
      val path = new File(tmpDir, "foos").getAbsolutePath

      val input = new Direct.Input(mkExtraConfigurations(tmpDir))("input", "input")

      prepareRound(path, configurePath)(0 until 100)

      val rc = newRoundContext()

      val result = Await.result(
        input.compute(rc).apply(Input).map {
          _().map {
            case (_, foo: Foo) => foo.id.get
          }.collect.toSeq.sorted
        }, Duration.Inf)

      assert(result === (0 until 100))

      assert(jobContext.inputStatistics(InputCounter.Direct).size === 1)
      val statistics = jobContext.inputStatistics(InputCounter.Direct)("input")
      assert(statistics.records === 100)
    }

    it should s"input from multiple paths: [${conf}]" in {
      implicit val jobContext = newJobContext(sc)

      val tmpDir = createTempDirectoryForEach("test-").toFile.getAbsolutePath
      val path1 = new File(tmpDir, "foos1").getAbsolutePath
      val path2 = new File(tmpDir, "foos2").getAbsolutePath

      val input = new Direct.Input(mkExtraConfigurations(tmpDir))("input", "input")

      prepareRound(path1, configurePath)(0 until 50)
      prepareRound(path2, configurePath)(50 until 100)

      val rc = newRoundContext()

      val result = Await.result(
        input.compute(rc).apply(Input).map {
          _().map {
            case (_, foo: Foo) => foo.id.get
          }.collect.toSeq.sorted
        }, Duration.Inf)

      assert(result === (0 until 100))

      assert(jobContext.inputStatistics(InputCounter.Direct).size === 1)
      val statistics = jobContext.inputStatistics(InputCounter.Direct)("input")
      assert(statistics.records === 100)
    }
  }

  private def mkExtraConfigurations(root: String): Map[String, String] = {
    Map(
      "com.asakusafw.directio.test" -> classOf[HadoopDataSource].getName,
      "com.asakusafw.directio.test.path" -> "test",
      "com.asakusafw.directio.test.fs.path" -> root,
      DirectFileInputFormat.KEY_BASE_PATH -> "test",
      DirectFileInputFormat.KEY_RESOURCE_PATH -> "foos*/part-*",
      DirectFileInputFormat.KEY_DATA_CLASS -> classOf[Foo].getName,
      DirectFileInputFormat.KEY_FORMAT_CLASS -> classOf[FooSequenceFileFormat].getName)
  }
}

@RunWith(classOf[JUnitRunner])
class DirectInputWithParallelismSpecTest extends DirectInputWithParallelismSpec

class DirectInputWithParallelismSpec extends DirectInputSpec {

  override def configure(conf: SparkConf): SparkConf = {
    conf.set("spark.default.parallelism", 8.toString)
  }
}

object InputSpec {

  val Input = BranchKey(0)

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

  object Foo {

    def intToFoo: Int => (NullWritable, Foo) = {

      lazy val foo = new Foo()

      { i =>
        foo.id.modify(i)
        (NullWritable.get, foo)
      }
    }
  }

  object Temporary {

    class Input(
      basePaths: Set[String])(
        val name: String,
        val label: String)(
          implicit jobContext: JobContext)
      extends TemporaryInput[Foo](Map.empty)
      with CacheOnce[RoundContext, Map[BranchKey, Future[() => RDD[_]]]] {

      def this(
        path: String)(
          name: String,
          label: String)(
            implicit jobContext: JobContext) = this(Set(path))(name, label)

      override def paths: Set[String] = basePaths.map(_ + "/part-*")

      override def branchKeys: Set[BranchKey] = Set(Input)

      override def partitioners: Map[BranchKey, Option[Partitioner]] = Map.empty

      override def orderings: Map[BranchKey, Ordering[ShuffleKey]] = Map.empty

      override def aggregations(
        broadcasts: Map[BroadcastId, Broadcasted[_]]): Map[BranchKey, Aggregation[ShuffleKey, _, _]] = Map.empty

      override def shuffleKey(branch: BranchKey, value: Any): ShuffleKey = null

      override def deserializerFor(branch: BranchKey): Array[Byte] => Any = { value =>
        ???
      }

      override def fragments(
        broadcasts: Map[BroadcastId, Broadcasted[_]])(
          fragmentBufferSize: Int): (Fragment[Foo], Map[BranchKey, OutputFragment[_]]) = {
        val fragment = new GenericOutputFragment[Foo](fragmentBufferSize)
        val outputs = Map(Input -> fragment)
        (fragment, outputs)
      }
    }
  }

  object Direct {

    class Input(
      val extraConfigurations: Map[String, String])(
        val name: String,
        val label: String)(
          implicit jobContext: JobContext)
      extends DirectInput(
        Map.empty)(
        classTag[DirectFileInputFormat].asInstanceOf[ClassTag[InputFormat[NullWritable, Foo]]],
        classTag[NullWritable], classTag[Foo], implicitly)
      with CacheOnce[RoundContext, Map[BranchKey, Future[() => RDD[_]]]] {

      override def branchKeys: Set[BranchKey] = Set(Input)

      override def partitioners: Map[BranchKey, Option[Partitioner]] = Map.empty

      override def orderings: Map[BranchKey, Ordering[ShuffleKey]] = Map.empty

      override def aggregations(
        broadcasts: Map[BroadcastId, Broadcasted[_]]): Map[BranchKey, Aggregation[ShuffleKey, _, _]] = Map.empty

      override def shuffleKey(branch: BranchKey, value: Any): ShuffleKey = null

      override def deserializerFor(branch: BranchKey): Array[Byte] => Any = { value =>
        ???
      }

      override def fragments(
        broadcasts: Map[BroadcastId, Broadcasted[_]])(
          fragmentBufferSize: Int): (Fragment[Foo], Map[BranchKey, OutputFragment[_]]) = {
        val fragment = new GenericOutputFragment[Foo](fragmentBufferSize)
        val outputs = Map(Input -> fragment)
        (fragment, outputs)
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
