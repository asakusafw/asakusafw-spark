package com.asakusafw.spark.compiler.operator
package user

import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner

import java.nio.file.Files
import java.util.{ List => JList }

import scala.collection.JavaConversions._

import com.asakusafw.lang.compiler.api.CompilerOptions
import com.asakusafw.lang.compiler.api.testing.MockJobflowProcessorContext
import com.asakusafw.lang.compiler.model.PropertyName
import com.asakusafw.lang.compiler.model.description._
import com.asakusafw.lang.compiler.model.graph.Group
import com.asakusafw.lang.compiler.model.testing.OperatorExtractor
import com.asakusafw.runtime.core.Result
import com.asakusafw.runtime.model.DataModel
import com.asakusafw.runtime.value._
import com.asakusafw.spark.compiler.spi.UserOperatorCompiler
import com.asakusafw.spark.runtime.driver.PrepareKey
import com.asakusafw.spark.runtime.fragment._
import com.asakusafw.spark.tools.asm._
import com.asakusafw.vocabulary.operator.Fold

@RunWith(classOf[JUnitRunner])
class FoldOperatorCompilerSpecTest extends FoldOperatorCompilerSpec

class FoldOperatorCompilerSpec extends FlatSpec with LoadClassSugar {

  import FoldOperatorCompilerSpec._

  behavior of classOf[FoldOperatorCompiler].getSimpleName

  def resolvers = UserOperatorCompiler(Thread.currentThread.getContextClassLoader)

  it should "compile Fold operator" in {
    val operator = OperatorExtractor
      .extract(classOf[Fold], classOf[FoldOperator], "fold")
      .input("hoges", ClassDescription.of(classOf[Hoge]),
        new Group(Seq(PropertyName.of("id")), Seq.empty[Group.Ordering]))
      .output("result", ClassDescription.of(classOf[Hoge]))
      .argument("n", ImmediateDescription.of(10))
      .build()

    val compiler = resolvers(classOf[Fold])
    val classpath = Files.createTempDirectory("FoldOperatorCompilerSpec").toFile
    val context = OperatorCompiler.Context(
      flowId = "flowId",
      jpContext = new MockJobflowProcessorContext(
        new CompilerOptions("buildid", "", Map.empty[String, String]),
        Thread.currentThread.getContextClassLoader,
        classpath))
    val thisType = compiler.compile(operator)(context)
    val cls = loadClass(thisType.getClassName, classpath).asSubclass(classOf[Fragment[Seq[Iterable[_]]]])

    val prepareKey = new PrepareIntOption()

    val result = {
      val builder = new OneToOneOutputFragmentClassBuilder(
        context.flowId, classOf[String].asType, classOf[Hoge].asType, classOf[IntOption].asType)
      val cls = loadClass(builder.thisType.getClassName, builder.build())
        .asSubclass(classOf[OneToOneOutputFragment[String, Hoge, IntOption]])
      cls.getConstructor(classOf[String], classOf[PrepareKey[_]])
        .newInstance("branch", prepareKey)
    }

    val fragment = cls.getConstructor(classOf[Fragment[_]]).newInstance(result)

    {
      val hoge = new Hoge()
      hoge.id.modify(1)
      hoge.price.modify(100)
      val hoges = Seq(hoge)
      fragment.add(Seq(hoges))
      assert(result.buffer.size === 1)
      assert(result.buffer(0)._1._1 === "branch")
      assert(result.buffer(0)._1._2.get === 1)
      assert(result.buffer(0)._2.id.get === 1)
      assert(result.buffer(0)._2.price.get === 100)
    }

    fragment.reset()
    assert(result.buffer.size === 0)

    {
      val hoges = (0 until 10).map { i =>
        val hoge = new Hoge()
        hoge.id.modify(1)
        hoge.price.modify(10 * i)
        hoge
      }
      fragment.add(Seq(hoges))
      assert(result.buffer.size === 1)
      assert(result.buffer(0)._1._1 === "branch")
      assert(result.buffer(0)._1._2.get === 1)
      assert(result.buffer(0)._2.id.get === 1)
      assert(result.buffer(0)._2.price.get === (0 until 10).map(_ * 10).sum + 10 * 9)
    }

    fragment.reset()
    assert(result.buffer.size === 0)
  }
}

object FoldOperatorCompilerSpec {

  class Hoge extends DataModel[Hoge] {

    val id = new IntOption()
    val price = new IntOption()

    override def reset(): Unit = {
      id.setNull()
      price.setNull()
    }
    override def copyFrom(other: Hoge): Unit = {
      id.copyFrom(other.id)
      price.copyFrom(other.price)
    }

    def getIdOption: IntOption = id
    def getPriceOption: IntOption = price
  }

  class FoldOperator {

    @Fold
    def fold(acc: Hoge, each: Hoge, n: Int): Unit = {
      acc.price.add(each.price)
      acc.price.add(n)
    }
  }

  class PrepareIntOption extends PrepareKey[String] {

    override def shuffleKey[U](branch: String, value: DataModel[_]): U = {
      value.asInstanceOf[Hoge].id.asInstanceOf[U]
    }
  }
}
