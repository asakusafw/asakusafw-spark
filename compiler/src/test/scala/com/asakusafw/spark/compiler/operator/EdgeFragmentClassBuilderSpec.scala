/*
 * Copyright 2011-2021 Asakusa Framework Team.
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

import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner

import java.io.{ DataInput, DataOutput }

import org.apache.hadoop.io.Writable

import com.asakusafw.runtime.model.DataModel
import com.asakusafw.runtime.value.IntOption
import com.asakusafw.spark.runtime.fragment.{ EdgeFragment, Fragment, GenericOutputFragment }
import com.asakusafw.spark.tools.asm._

@RunWith(classOf[JUnitRunner])
class EdgeFragmentClassBuilderSpecTest extends EdgeFragmentClassBuilderSpec

class EdgeFragmentClassBuilderSpec extends FlatSpec with UsingCompilerContext {

  import EdgeFragmentClassBuilderSpec._

  behavior of classOf[EdgeFragmentClassBuilder].getSimpleName

  it should "compile EdgeFragment" in {
    val out1 = new GenericOutputFragment[Foo]()
    val out2 = new GenericOutputFragment[Foo]()

    implicit val context = newOperatorCompilerContext("flowId")

    val thisType = EdgeFragmentClassBuilder.getOrCompile(classOf[Foo].asType)
    val cls = context.loadClass[EdgeFragment[Foo]](thisType.getClassName)

    val fragment = cls.getConstructor(classOf[Array[Fragment[_]]]).newInstance(Array(out1, out2))

    fragment.reset()
    val foo = new Foo()
    for (i <- 0 until 10) {
      foo.i.modify(i)
      fragment.add(foo)
    }
    out1.iterator.zipWithIndex.foreach {
      case (foo, i) =>
        assert(foo.i.get === i)
    }
    out2.iterator.zipWithIndex.foreach {
      case (foo, i) =>
        assert(foo.i.get === i)
    }

    fragment.reset()
  }
}

object EdgeFragmentClassBuilderSpec {

  class Foo extends DataModel[Foo] with Writable {

    val i: IntOption = new IntOption()

    override def reset: Unit = {
      i.setNull()
    }
    override def copyFrom(other: Foo): Unit = {
      i.copyFrom(other.i)
    }
    override def readFields(in: DataInput): Unit = {
      i.readFields(in)
    }
    override def write(out: DataOutput): Unit = {
      i.write(out)
    }
  }
}
