package com.asakusafw.spark.runtime.serializer

import org.apache.hadoop.conf.Configuration
import org.apache.spark.serializer.{ KryoRegistrator => SparkKryoRegistrator }
import org.slf4j.LoggerFactory

import com.esotericsoftware.kryo.Kryo

import com.asakusafw.runtime.value._
import com.asakusafw.spark.runtime.driver.ShuffleKey
import com.asakusafw.spark.runtime.io.BufferSlice
import com.asakusafw.spark.runtime.rdd.Branch

class KryoRegistrator extends SparkKryoRegistrator {

  import KryoRegistrator._

  override def registerClasses(kryo: Kryo): Unit = {
    kryo.register(classOf[Configuration], WritableSerializer.ConfigurationSerializer)

    yarnConfSerializer.foreach {
      case (yarnConfClass, serializer) =>
        kryo.register(yarnConfClass, serializer)
    }

    kryo.register(classOf[Branch[_]], BranchSerializer)
    kryo.register(classOf[ShuffleKey], ShuffleKeySerializer)
    kryo.register(classOf[BufferSlice], BufferSliceSerializer)

    kryo.register(classOf[BooleanOption], WritableSerializer.BooleanOptionSerializer)
    kryo.register(classOf[ByteOption], WritableSerializer.ByteOptionSerializer)
    kryo.register(classOf[ShortOption], WritableSerializer.ShortOptionSerializer)
    kryo.register(classOf[IntOption], WritableSerializer.IntOptionSerializer)
    kryo.register(classOf[LongOption], WritableSerializer.LongOptionSerializer)
    kryo.register(classOf[FloatOption], WritableSerializer.FloatOptionSerializer)
    kryo.register(classOf[DoubleOption], WritableSerializer.DoubleOptionSerializer)
    kryo.register(classOf[DecimalOption], WritableSerializer.DecimalOptionSerializer)
    kryo.register(classOf[StringOption], WritableSerializer.StringOptionSerializer)
    kryo.register(classOf[DateOption], WritableSerializer.DateOptionSerializer)
    kryo.register(classOf[DateTimeOption], WritableSerializer.DateTimeOptionSerializer)
  }
}

object KryoRegistrator {

  val Logger = LoggerFactory.getLogger(getClass)

  val yarnConfSerializer: Option[(Class[_ <: Configuration], WritableSerializer[Configuration])] = {
    try {
      val yarnConfClass =
        Class.forName(
          "org.apache.hadoop.yarn.conf.YarnConfiguration",
          false,
          Option(Thread.currentThread.getContextClassLoader).getOrElse(getClass.getClassLoader))
          .asSubclass(classOf[Configuration])
      if (Logger.isDebugEnabled) {
        Logger.debug(s"${yarnConfClass} is found.")
      }
      Some(
        yarnConfClass,
        new WritableSerializer[Configuration] {
          override def newInstance(): Configuration = yarnConfClass.newInstance()
        })
    } catch {
      case e: ClassNotFoundException =>
        if (Logger.isDebugEnabled) {
          Logger.debug(e.getMessage, e)
        }
        None
    }
  }
}
