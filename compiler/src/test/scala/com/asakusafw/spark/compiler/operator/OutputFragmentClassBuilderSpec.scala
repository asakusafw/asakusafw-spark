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
package operator

import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner

import java.io.{ DataInput, DataOutput }
import java.lang.{ Long => JLong }

import org.apache.hadoop.io.Writable

import com.asakusafw.runtime.model.DataModel
import com.asakusafw.runtime.value._
import com.asakusafw.spark.runtime.fragment.OutputFragment
import com.asakusafw.spark.tools.asm._

@RunWith(classOf[JUnitRunner])
class OutputFragmentClassBuilderSpecTest extends OutputFragmentClassBuilderSpec

class OutputFragmentClassBuilderSpec extends FlatSpec with LoadClassSugar with TempDir with CompilerContext {

  import OutputFragmentClassBuilderSpec._

  behavior of classOf[OutputFragmentClassBuilder].getSimpleName

  it should "compile OutputFragment" in {
    val classpath = createTempDirectory("OutputFragmentClassBuilderSpec").toFile
    implicit val context = newContext("flowId", classpath)

    val builder = new OutputFragmentClassBuilder(classOf[TestModel].asType)
    val cls = loadClass(builder.thisType.getClassName, builder.build())
      .asSubclass(classOf[OutputFragment[TestModel]])

    val fragment = cls.getConstructor(classOf[Int]).newInstance(Int.box(-1))

    fragment.reset()
    val dm = new TestModel()
    for (i <- 0 until 10) {
      dm.i.modify(i)
      fragment.add(dm)
    }
    fragment.iterator.zipWithIndex.foreach {
      case (dm, i) =>
        assert(dm.i.get === i)
    }

    fragment.reset()
  }
}

object OutputFragmentClassBuilderSpec {

  class TestModel extends DataModel[TestModel] with Writable {

    val i: IntOption = new IntOption()

    override def reset: Unit = {
      i.setNull()
    }
    override def copyFrom(other: TestModel): Unit = {
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
