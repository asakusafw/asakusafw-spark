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

  behavior of classOf[OrderingClassBuilder].getSimpleName

  def resolvers = OrderingCompiler(Thread.currentThread.getContextClassLoader)

  it should "build data model ordering class" in {
    val builder = OrderingClassBuilder(
      "flowId", Seq(
        Type.BOOLEAN_TYPE,
        Type.BYTE_TYPE, Type.CHAR_TYPE, Type.SHORT_TYPE, Type.INT_TYPE,
        Type.LONG_TYPE, Type.FLOAT_TYPE, Type.DOUBLE_TYPE,
        classOf[BigInt].asType, classOf[BigDecimal].asType,
        classOf[String].asType,
        classOf[BooleanOption].asType,
        classOf[ByteOption].asType, classOf[ShortOption].asType, classOf[IntOption].asType, classOf[LongOption].asType,
        classOf[FloatOption].asType, classOf[DoubleOption].asType, classOf[DecimalOption].asType,
        classOf[StringOption].asType,
        classOf[DateOption].asType,
        classOf[DateTimeOption].asType),
      resolvers)
    val ordering = loadClass(builder.thisType.getClassName, builder.build()).asSubclass(classOf[Ordering[Seq[Any]]]).newInstance
    val x = Seq(
      false,
      0.toByte, 0.toChar, 0.toShort, 0, 0L,
      0.0f, 0.0d, BigInt(0), BigDecimal(0),
      "",
      new BooleanOption(),
      new ByteOption(),
      new ShortOption(),
      new IntOption(),
      new LongOption(),
      new FloatOption(),
      new DoubleOption(),
      new DecimalOption(),
      new StringOption(),
      new DateOption(),
      new DateTimeOption())
    assert(ordering.compare(x, x) === 0)

    val y = Seq(
      true,
      0.toByte, 0.toChar, 0.toShort, 0, 0L,
      0.0f, 0.0d, BigInt(0), BigDecimal(0),
      "",
      new BooleanOption(),
      new ByteOption(),
      new ShortOption(),
      new IntOption(),
      new LongOption(),
      new FloatOption(),
      new DoubleOption(),
      new DecimalOption(),
      new StringOption(),
      new DateOption(),
      new DateTimeOption())
    assert(ordering.compare(x, y) < 0)

    val z = Seq(
      false,
      0.toByte, 0.toChar, 0.toShort, 0, 0L,
      0.0f, 0.0d, BigInt(0), BigDecimal(0),
      "",
      new BooleanOption(),
      new ByteOption(),
      new ShortOption(),
      new IntOption(),
      new LongOption(),
      new FloatOption(),
      new DoubleOption(),
      new DecimalOption(),
      new StringOption(),
      new DateOption(),
      new DateTimeOption(),
      true)
    assert(ordering.compare(x, z) === 0)
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
