package com.asakusafw.spark.compiler.spi

import scala.math.BigDecimal

import org.objectweb.asm.Type

import com.asakusafw.runtime.value
import com.asakusafw.spark.runtime.orderings
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._

trait OrderingCompiler {

  def of: Type
  def compare(x: Var, y: Var)(implicit mb: MethodBuilder): Stack
}

object OrderingCompiler {

  class Boolean extends OrderingCompiler {
    def of: Type = Type.BOOLEAN_TYPE
    def compare(x: Var, y: Var)(implicit mb: MethodBuilder): Stack = {
      import mb._
      assert(x.`type` == of)
      assert(y.`type` == of)
      getStatic(Ordering.Boolean.getClass.asType, "MODULE$", Ordering.Boolean.getClass.asType)
        .invokeV("compare", Type.INT_TYPE, x.push(), y.push())
    }
  }

  class Byte extends OrderingCompiler {
    def of: Type = Type.BYTE_TYPE
    def compare(x: Var, y: Var)(implicit mb: MethodBuilder): Stack = {
      import mb._
      assert(x.`type` == of)
      assert(y.`type` == of)
      getStatic(Ordering.Byte.getClass.asType, "MODULE$", Ordering.Byte.getClass.asType)
        .invokeV("compare", Type.INT_TYPE, x.push(), y.push())
    }
  }

  class Char extends OrderingCompiler {
    def of: Type = Type.CHAR_TYPE
    def compare(x: Var, y: Var)(implicit mb: MethodBuilder): Stack = {
      import mb._
      assert(x.`type` == of)
      assert(y.`type` == of)
      getStatic(Ordering.Char.getClass.asType, "MODULE$", Ordering.Char.getClass.asType)
        .invokeV("compare", Type.INT_TYPE, x.push(), y.push())
    }
  }

  class Short extends OrderingCompiler {
    def of: Type = Type.SHORT_TYPE
    def compare(x: Var, y: Var)(implicit mb: MethodBuilder): Stack = {
      import mb._
      assert(x.`type` == of)
      assert(y.`type` == of)
      getStatic(Ordering.Short.getClass.asType, "MODULE$", Ordering.Short.getClass.asType)
        .invokeV("compare", Type.INT_TYPE, x.push(), y.push())
    }
  }

  class Int extends OrderingCompiler {
    def of: Type = Type.INT_TYPE
    def compare(x: Var, y: Var)(implicit mb: MethodBuilder): Stack = {
      import mb._
      assert(x.`type` == of)
      assert(y.`type` == of)
      getStatic(Ordering.Int.getClass.asType, "MODULE$", Ordering.Int.getClass.asType)
        .invokeV("compare", Type.INT_TYPE, x.push(), y.push())
    }
  }

  class Long extends OrderingCompiler {
    def of: Type = Type.LONG_TYPE
    def compare(x: Var, y: Var)(implicit mb: MethodBuilder): Stack = {
      import mb._
      assert(x.`type` == of)
      assert(y.`type` == of)
      getStatic(Ordering.Long.getClass.asType, "MODULE$", Ordering.Long.getClass.asType)
        .invokeV("compare", Type.INT_TYPE, x.push(), y.push())
    }
  }

  class Float extends OrderingCompiler {
    def of: Type = Type.FLOAT_TYPE
    def compare(x: Var, y: Var)(implicit mb: MethodBuilder): Stack = {
      import mb._
      assert(x.`type` == of)
      assert(y.`type` == of)
      getStatic(Ordering.Float.getClass.asType, "MODULE$", Ordering.Float.getClass.asType)
        .invokeV("compare", Type.INT_TYPE, x.push(), y.push())
    }
  }

  class Double extends OrderingCompiler {
    def of: Type = Type.DOUBLE_TYPE
    def compare(x: Var, y: Var)(implicit mb: MethodBuilder): Stack = {
      import mb._
      assert(x.`type` == of)
      assert(y.`type` == of)
      getStatic(Ordering.Double.getClass.asType, "MODULE$", Ordering.Double.getClass.asType)
        .invokeV("compare", Type.INT_TYPE, x.push(), y.push())
    }
  }

  class BigInt extends OrderingCompiler {
    def of: Type = classOf[scala.math.BigInt].asType
    def compare(x: Var, y: Var)(implicit mb: MethodBuilder): Stack = {
      import mb._
      assert(x.`type` == of)
      assert(y.`type` == of)
      getStatic(Ordering.BigInt.getClass.asType, "MODULE$", Ordering.BigInt.getClass.asType)
        .invokeV("compare", Type.INT_TYPE, x.push(), y.push())
    }
  }

  class BigDecimal extends OrderingCompiler {
    def of: Type = classOf[scala.math.BigDecimal].asType
    def compare(x: Var, y: Var)(implicit mb: MethodBuilder): Stack = {
      import mb._
      assert(x.`type` == of)
      assert(y.`type` == of)
      getStatic(Ordering.BigDecimal.getClass.asType, "MODULE$", Ordering.BigDecimal.getClass.asType)
        .invokeV("compare", Type.INT_TYPE, x.push(), y.push())
    }
  }

  class String extends OrderingCompiler {
    def of: Type = classOf[java.lang.String].asType
    def compare(x: Var, y: Var)(implicit mb: MethodBuilder): Stack = {
      import mb._
      assert(x.`type` == of)
      assert(y.`type` == of)
      getStatic(Ordering.String.getClass.asType, "MODULE$", Ordering.String.getClass.asType)
        .invokeV("compare", Type.INT_TYPE, x.push(), y.push())
    }
  }

  class BooleanOption extends OrderingCompiler {
    def of: Type = classOf[value.BooleanOption].asType
    def compare(x: Var, y: Var)(implicit mb: MethodBuilder): Stack = {
      import mb._
      assert(x.`type` == of)
      assert(y.`type` == of)
      getStatic(orderings.BooleanOption.getClass.asType, "MODULE$", orderings.BooleanOption.getClass.asType)
        .invokeV("compare", Type.INT_TYPE, x.push(), y.push())
    }
  }

  class ByteOption extends OrderingCompiler {
    def of: Type = classOf[value.ByteOption].asType
    def compare(x: Var, y: Var)(implicit mb: MethodBuilder): Stack = {
      import mb._
      assert(x.`type` == of)
      assert(y.`type` == of)
      getStatic(orderings.ByteOption.getClass.asType, "MODULE$", orderings.ByteOption.getClass.asType)
        .invokeV("compare", Type.INT_TYPE, x.push(), y.push())
    }
  }

  class ShortOption extends OrderingCompiler {
    def of: Type = classOf[value.ShortOption].asType
    def compare(x: Var, y: Var)(implicit mb: MethodBuilder): Stack = {
      import mb._
      assert(x.`type` == of)
      assert(y.`type` == of)
      getStatic(orderings.ShortOption.getClass.asType, "MODULE$", orderings.ShortOption.getClass.asType)
        .invokeV("compare", Type.INT_TYPE, x.push(), y.push())
    }
  }

  class IntOption extends OrderingCompiler {
    def of: Type = classOf[value.IntOption].asType
    def compare(x: Var, y: Var)(implicit mb: MethodBuilder): Stack = {
      import mb._
      assert(x.`type` == of)
      assert(y.`type` == of)
      getStatic(orderings.IntOption.getClass.asType, "MODULE$", orderings.IntOption.getClass.asType)
        .invokeV("compare", Type.INT_TYPE, x.push(), y.push())
    }
  }

  class LongOption extends OrderingCompiler {
    def of: Type = classOf[value.LongOption].asType
    def compare(x: Var, y: Var)(implicit mb: MethodBuilder): Stack = {
      import mb._
      assert(x.`type` == of)
      assert(y.`type` == of)
      getStatic(orderings.LongOption.getClass.asType, "MODULE$", orderings.LongOption.getClass.asType)
        .invokeV("compare", Type.INT_TYPE, x.push(), y.push())
    }
  }

  class FloatOption extends OrderingCompiler {
    def of: Type = classOf[value.FloatOption].asType
    def compare(x: Var, y: Var)(implicit mb: MethodBuilder): Stack = {
      import mb._
      assert(x.`type` == of)
      assert(y.`type` == of)
      getStatic(orderings.FloatOption.getClass.asType, "MODULE$", orderings.FloatOption.getClass.asType)
        .invokeV("compare", Type.INT_TYPE, x.push(), y.push())
    }
  }

  class DoubleOption extends OrderingCompiler {
    def of: Type = classOf[value.DoubleOption].asType
    def compare(x: Var, y: Var)(implicit mb: MethodBuilder): Stack = {
      import mb._
      assert(x.`type` == of)
      assert(y.`type` == of)
      getStatic(orderings.DoubleOption.getClass.asType, "MODULE$", orderings.DoubleOption.getClass.asType)
        .invokeV("compare", Type.INT_TYPE, x.push(), y.push())
    }
  }

  class DecimalOption extends OrderingCompiler {
    def of: Type = classOf[value.DecimalOption].asType
    def compare(x: Var, y: Var)(implicit mb: MethodBuilder): Stack = {
      import mb._
      assert(x.`type` == of)
      assert(y.`type` == of)
      getStatic(orderings.DecimalOption.getClass.asType, "MODULE$", orderings.DecimalOption.getClass.asType)
        .invokeV("compare", Type.INT_TYPE, x.push(), y.push())
    }
  }

  class StringOption extends OrderingCompiler {
    def of: Type = classOf[value.StringOption].asType
    def compare(x: Var, y: Var)(implicit mb: MethodBuilder): Stack = {
      import mb._
      assert(x.`type` == of)
      assert(y.`type` == of)
      getStatic(orderings.StringOption.getClass.asType, "MODULE$", orderings.StringOption.getClass.asType)
        .invokeV("compare", Type.INT_TYPE, x.push(), y.push())
    }
  }

  class DateOption extends OrderingCompiler {
    def of: Type = classOf[value.DateOption].asType
    def compare(x: Var, y: Var)(implicit mb: MethodBuilder): Stack = {
      import mb._
      assert(x.`type` == of)
      assert(y.`type` == of)
      getStatic(orderings.DateOption.getClass.asType, "MODULE$", orderings.DateOption.getClass.asType)
        .invokeV("compare", Type.INT_TYPE, x.push(), y.push())
    }
  }

  class DateTimeOption extends OrderingCompiler {
    def of: Type = classOf[value.DateTimeOption].asType
    def compare(x: Var, y: Var)(implicit mb: MethodBuilder): Stack = {
      import mb._
      assert(x.`type` == of)
      assert(y.`type` == of)
      getStatic(orderings.DateTimeOption.getClass.asType, "MODULE$", orderings.DateTimeOption.getClass.asType)
        .invokeV("compare", Type.INT_TYPE, x.push(), y.push())
    }
  }
}
