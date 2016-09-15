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
package com.asakusafw.spark.compiler
package operator
package user

import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner

import java.io.{ DataInput, DataOutput }

import scala.collection.JavaConversions._
import scala.collection.mutable

import org.apache.hadoop.io.Writable
import org.apache.spark.broadcast.Broadcast

import com.asakusafw.bridge.broker.{ ResourceBroker, ResourceSession }
import com.asakusafw.lang.compiler.model.description.{ ClassDescription, ImmediateDescription }
import com.asakusafw.lang.compiler.model.testing.OperatorExtractor
import com.asakusafw.runtime.core.{ HadoopConfiguration, Report, ResourceConfiguration }
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.runtime.value.{ IntOption, LongOption }
import com.asakusafw.spark.compiler.spi.{ OperatorCompiler, OperatorType }
import com.asakusafw.spark.runtime.fragment.{ Fragment, GenericOutputFragment }
import com.asakusafw.spark.runtime.graph.BroadcastId
import com.asakusafw.spark.tools.asm._
import com.asakusafw.vocabulary.operator.Logging

@RunWith(classOf[JUnitRunner])
class LoggingOperatorCompilerSpecTest extends LoggingOperatorCompilerSpec

class LoggingOperatorCompilerSpec extends FlatSpec with UsingCompilerContext {

  import LoggingOperatorCompilerSpec._

  behavior of classOf[LoggingOperatorCompiler].getSimpleName

  for {
    level <- Logging.Level.values
  } {
    it should s"compile Logging operator: Logging.Level.${level}" in {
      val operator = OperatorExtractor
        .extract(classOf[Logging], classOf[LoggingOperator], s"logging_${level.name.toLowerCase}")
        .input("in", ClassDescription.of(classOf[Foo]))
        .output("out", ClassDescription.of(classOf[Foo]))
        .argument("n", ImmediateDescription.of(10))
        .build()

      implicit val context = newOperatorCompilerContext("flowId")

      val thisType = OperatorCompiler.compile(operator, OperatorType.ExtractType)
      val cls = context.loadClass[Fragment[Foo]](thisType.getClassName)

      val out = new GenericOutputFragment[Foo]()

      withResourceBroker {
        val fragment =
          cls.getConstructor(classOf[Map[BroadcastId, Broadcast[_]]], classOf[Fragment[_]])
            .newInstance(Map.empty, out)
        fragment.reset()

        val foo = new Foo()
        for (i <- 0 until 10) {
          foo.i.modify(i)
          foo.l.modify(i * 10)
          fragment.add(foo)
        }
        out.iterator.zipWithIndex.foreach {
          case (output, i) =>
            assert(output.i.get === i)
            assert(output.l.get === i * 10)
        }

        fragment.reset()

        assert(Logs.get() ===
          (0 until 10).map(i =>
            (level match {
              case Logging.Level.ERROR => "ERROR"
              case Logging.Level.WARN => "WARN"
              case _ => "INFO"
            },
              s"[${level.name}] foo: Foo(${i}, ${i * 10}), n: 10")))
      }
    }

    it should s"compile Extract operator with projective model: Logging.Level.${level}" in {
      val operator = OperatorExtractor
        .extract(classOf[Logging], classOf[LoggingOperator], s"loggingp_${level.name.toLowerCase}")
        .input("in", ClassDescription.of(classOf[Foo]))
        .output("out", ClassDescription.of(classOf[Foo]))
        .argument("n", ImmediateDescription.of(10))
        .build()

      implicit val context = newOperatorCompilerContext("flowId")

      val thisType = OperatorCompiler.compile(operator, OperatorType.ExtractType)
      val cls = context.loadClass[Fragment[Foo]](thisType.getClassName)

      val out = new GenericOutputFragment[Foo]()

      withResourceBroker {
        val fragment =
          cls.getConstructor(classOf[Map[BroadcastId, Broadcast[_]]], classOf[Fragment[_]])
            .newInstance(Map.empty, out)

        fragment.reset()
        val foo = new Foo()
        for (i <- 0 until 10) {
          foo.i.modify(i)
          foo.l.modify(i * 10)
          fragment.add(foo)
        }
        out.iterator.zipWithIndex.foreach {
          case (output, i) =>
            assert(output.i.get === i)
            assert(output.l.get === i * 10)
        }

        fragment.reset()

        assert(Logs.get() ===
          (0 until 10).map(i =>
            (level match {
              case Logging.Level.ERROR => "ERROR"
              case Logging.Level.WARN => "WARN"
              case _ => "INFO"
            },
              s"[${level.name}] f: F(${i}), n: 10")))
      }
    }
  }

  private def withResourceBroker(block: => Unit): Unit = {
    val session = ResourceBroker.attach(
      ResourceBroker.Scope.THREAD,
      new ResourceBroker.Initializer {
        override def accept(session: ResourceSession): Unit = {
          val conf = new HadoopConfiguration()
          conf.set("com.asakusafw.runtime.core.Report.Delegate", classOf[Delegate].getName)
          session.put(classOf[ResourceConfiguration], conf)
        }
      })
    try {
      block
    } finally {
      session.close()
    }
  }
}

object LoggingOperatorCompilerSpec {

  val Logs = new ThreadLocal[mutable.ArrayBuffer[(String, String)]] {
    override def initialValue() = mutable.ArrayBuffer.empty
  }

  class Delegate extends Report.Delegate {

    override def report(level: Report.Level, message: String): Unit = {
      Logs.get() += ((level.name, message))
    }

    override def cleanup(conf: ResourceConfiguration): Unit = {
      Logs.get().clear()
    }
  }

  trait FooP {
    def getIOption: IntOption
  }

  class Foo extends DataModel[Foo] with FooP with Writable {

    val i: IntOption = new IntOption()
    val l: LongOption = new LongOption()

    override def reset: Unit = {
      i.setNull()
      l.setNull()
    }
    override def copyFrom(other: Foo): Unit = {
      i.copyFrom(other.i)
      l.copyFrom(other.l)
    }
    override def readFields(in: DataInput): Unit = {
      i.readFields(in)
      l.readFields(in)
    }
    override def write(out: DataOutput): Unit = {
      i.write(out)
      l.write(out)
    }

    def getIOption: IntOption = i
    def getLOption: LongOption = l
  }

  class LoggingOperator {

    @Logging(Logging.Level.ERROR)
    def logging_error(
      foo: Foo,
      n: Int): String = {
      s"[ERROR] foo: Foo(${foo.i.get}, ${foo.l.get}), n: ${n}"
    }

    @Logging(Logging.Level.ERROR)
    def loggingp_error[F <: FooP](
      f: F,
      n: Int): String = {
      s"[ERROR] f: F(${f.getIOption.get}), n: ${n}"
    }

    @Logging(Logging.Level.WARN)
    def logging_warn(
      foo: Foo,
      n: Int): String = {
      s"[WARN] foo: Foo(${foo.i.get}, ${foo.l.get}), n: ${n}"
    }

    @Logging(Logging.Level.WARN)
    def loggingp_warn[F <: FooP](
      f: F,
      n: Int): String = {
      s"[WARN] f: F(${f.getIOption.get}), n: ${n}"
    }

    @Logging(Logging.Level.INFO)
    def logging_info(
      foo: Foo,
      n: Int): String = {
      s"[INFO] foo: Foo(${foo.i.get}, ${foo.l.get}), n: ${n}"
    }

    @Logging(Logging.Level.INFO)
    def loggingp_info[F <: FooP](
      f: F,
      n: Int): String = {
      s"[INFO] f: F(${f.getIOption.get}), n: ${n}"
    }

    @Logging(Logging.Level.DEBUG)
    def logging_debug(
      foo: Foo,
      n: Int): String = {
      s"[DEBUG] foo: Foo(${foo.i.get}, ${foo.l.get}), n: ${n}"
    }

    @Logging(Logging.Level.DEBUG)
    def loggingp_debug[F <: FooP](
      f: F,
      n: Int): String = {
      s"[DEBUG] f: F(${f.getIOption.get}), n: ${n}"
    }
  }
}
