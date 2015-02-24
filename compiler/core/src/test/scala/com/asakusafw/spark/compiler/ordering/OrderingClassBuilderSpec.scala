package com.asakusafw.spark.compiler.ordering

import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner

import org.objectweb.asm.Type

import com.asakusafw.runtime.model.DataModel
import com.asakusafw.runtime.value._
import com.asakusafw.spark.compiler.spi.OrderingCompiler
import com.asakusafw.spark.tools.asm._

@RunWith(classOf[JUnitRunner])
class OrderingClassBuilderSpecTest extends OrderingClassBuilderSpec

class OrderingClassBuilderSpec extends FlatSpec with LoadClassSugar {

  import OrderingClassBuilderSpec._

  behavior of classOf[OrderingClassBuilder[_]].getSimpleName

  def resolvers = OrderingCompiler(Thread.currentThread.getContextClassLoader)

  it should "build data model ordering class" in {
    val dataModelType = classOf[TestModel].asType
    val builder = OrderingClassBuilder[TestModel](
      dataModelType, Seq(
        ("z", Type.getMethodType(Type.BOOLEAN_TYPE)),
        ("b", Type.getMethodType(Type.BYTE_TYPE)),
        ("c", Type.getMethodType(Type.CHAR_TYPE)),
        ("s", Type.getMethodType(Type.SHORT_TYPE)),
        ("i", Type.getMethodType(Type.INT_TYPE)),
        ("j", Type.getMethodType(Type.LONG_TYPE)),
        ("f", Type.getMethodType(Type.FLOAT_TYPE)),
        ("d", Type.getMethodType(Type.DOUBLE_TYPE)),
        ("bi", Type.getMethodType(classOf[BigInt].asType)),
        ("bd", Type.getMethodType(classOf[BigDecimal].asType)),
        ("str", Type.getMethodType(classOf[String].asType)),
        ("zOpt", Type.getMethodType(classOf[BooleanOption].asType)),
        ("bOpt", Type.getMethodType(classOf[ByteOption].asType)),
        ("sOpt", Type.getMethodType(classOf[ShortOption].asType)),
        ("iOpt", Type.getMethodType(classOf[IntOption].asType)),
        ("jOpt", Type.getMethodType(classOf[LongOption].asType)),
        ("fOpt", Type.getMethodType(classOf[FloatOption].asType)),
        ("dOpt", Type.getMethodType(classOf[DoubleOption].asType)),
        ("decOpt", Type.getMethodType(classOf[DecimalOption].asType)),
        ("strOpt", Type.getMethodType(classOf[StringOption].asType)),
        ("dateOpt", Type.getMethodType(classOf[DateOption].asType)),
        ("dtOpt", Type.getMethodType(classOf[DateTimeOption].asType))),
      resolvers)
    val ordering = loadClass(builder.thisType.getClassName, builder.build()).asSubclass(classOf[Ordering[TestModel]]).newInstance
    val x = new TestModel
    val y = new TestModel
    assert(ordering.compare(x, y) === 0)
  }
}

object OrderingClassBuilderSpec {

  class TestModel extends DataModel[TestModel] {

    var z: Boolean = false
    var b: Byte = 0
    var c: Char = 0
    var s: Short = 0
    var i: Int = 0
    var j: Long = 0
    var f: Float = 0
    var d: Double = 0
    var bi: BigInt = BigInt(0)
    var bd: BigDecimal = BigDecimal(0)
    var str: String = ""
    val zOpt: BooleanOption = new BooleanOption()
    val bOpt: ByteOption = new ByteOption()
    val sOpt: ShortOption = new ShortOption()
    val iOpt: IntOption = new IntOption()
    val jOpt: LongOption = new LongOption()
    val fOpt: FloatOption = new FloatOption()
    val dOpt: DoubleOption = new DoubleOption()
    val decOpt: DecimalOption = new DecimalOption()
    val strOpt: StringOption = new StringOption()
    val dateOpt: DateOption = new DateOption()
    val dtOpt: DateTimeOption = new DateTimeOption()

    override def reset: Unit = {
      z = false
      b = 0
      c = '\0'
      s = 0
      i = 0
      j = 0
      f = 0
      d = 0
      bi = null
      bd = null
      str = null
      zOpt.setNull()
      bOpt.setNull()
      sOpt.setNull()
      iOpt.setNull()
      jOpt.setNull()
      fOpt.setNull()
      dOpt.setNull()
      decOpt.setNull()
      strOpt.setNull()
      dateOpt.setNull()
      dtOpt.setNull()
    }

    override def copyFrom(other: TestModel): Unit = {
      z = other.z
      b = other.b
      c = other.c
      s = other.s
      i = other.i
      j = other.j
      f = other.f
      d = other.d
      bi = other.bi
      bd = other.bd
      str = other.str
      zOpt.copyFrom(other.zOpt)
      bOpt.copyFrom(other.bOpt)
      sOpt.copyFrom(other.sOpt)
      iOpt.copyFrom(other.iOpt)
      jOpt.copyFrom(other.jOpt)
      fOpt.copyFrom(other.fOpt)
      dOpt.copyFrom(other.dOpt)
      decOpt.copyFrom(other.decOpt)
      strOpt.copyFrom(other.strOpt)
      dateOpt.copyFrom(other.dateOpt)
      dtOpt.copyFrom(other.dtOpt)
    }
  }
}
