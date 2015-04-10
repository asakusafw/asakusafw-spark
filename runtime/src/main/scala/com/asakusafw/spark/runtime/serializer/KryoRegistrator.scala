package com.asakusafw.spark.runtime.serializer

import org.apache.hadoop.conf.Configuration
import org.apache.spark.serializer.{ KryoRegistrator => SparkKryoRegistrator }
import org.slf4j.LoggerFactory

import com.esotericsoftware.kryo.Kryo

import com.asakusafw.runtime.value._
import com.asakusafw.spark.runtime.driver.ShuffleKey

class KryoRegistrator extends SparkKryoRegistrator {

  import KryoRegistrator._

  override def registerClasses(kryo: Kryo): Unit = {
    kryo.register(
      classOf[Configuration],
      new WritableSerializer[Configuration] {
        override def newInstance: Configuration = new Configuration()
      })
    yarnConf.foreach { yarnConfClass =>
      kryo.register(
        yarnConfClass,
        new WritableSerializer[Configuration] {
          override def newInstance: Configuration = yarnConfClass.newInstance()
        })
    }

    kryo.addDefaultSerializer(classOf[ShuffleKey], new ShuffleKeySerializer)
    kryo.register(classOf[ShuffleKey])

    kryo.register(
      classOf[BooleanOption],
      new ValueOptionSerializer[BooleanOption] {
        override def newInstance: BooleanOption = new BooleanOption()
      })
    kryo.register(
      classOf[ByteOption],
      new ValueOptionSerializer[ByteOption] {
        override def newInstance: ByteOption = new ByteOption()
      })
    kryo.register(
      classOf[ShortOption],
      new ValueOptionSerializer[ShortOption] {
        override def newInstance: ShortOption = new ShortOption()
      })
    kryo.register(
      classOf[IntOption],
      new ValueOptionSerializer[IntOption] {
        override def newInstance: IntOption = new IntOption()
      })
    kryo.register(
      classOf[LongOption],
      new ValueOptionSerializer[LongOption] {
        override def newInstance: LongOption = new LongOption()
      })
    kryo.register(
      classOf[FloatOption],
      new ValueOptionSerializer[FloatOption] {
        override def newInstance: FloatOption = new FloatOption()
      })
    kryo.register(
      classOf[DoubleOption],
      new ValueOptionSerializer[DoubleOption] {
        override def newInstance: DoubleOption = new DoubleOption()
      })
    kryo.register(
      classOf[DecimalOption],
      new ValueOptionSerializer[DecimalOption] {
        override def newInstance: DecimalOption = new DecimalOption()
      })
    kryo.register(
      classOf[StringOption],
      new ValueOptionSerializer[StringOption] {
        override def newInstance: StringOption = new StringOption()
      })
    kryo.register(
      classOf[DateOption],
      new ValueOptionSerializer[DateOption] {
        override def newInstance: DateOption = new DateOption()
      })
    kryo.register(
      classOf[DateTimeOption],
      new ValueOptionSerializer[DateTimeOption] {
        override def newInstance: DateTimeOption = new DateTimeOption()
      })
  }
}

object KryoRegistrator {

  val Logger = LoggerFactory.getLogger(getClass)

  val yarnConf: Option[Class[_ <: Configuration]] = {
    try {
      val yarnConfClass =
        Class.forName("org.apache.hadoop.yarn.conf.YarnConfiguration")
          .asSubclass(classOf[Configuration])
      if (Logger.isDebugEnabled) {
        Logger.debug(s"${yarnConfClass} is found.")
      }
      Some(yarnConfClass)
    } catch {
      case e: ClassNotFoundException =>
        if (Logger.isDebugEnabled) {
          Logger.debug(e.getMessage, e)
        }
        None
    }
  }
}
