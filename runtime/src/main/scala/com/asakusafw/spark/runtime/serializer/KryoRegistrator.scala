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
package com.asakusafw.spark.runtime.serializer

import org.apache.hadoop.conf.Configuration
import org.apache.spark.serializer.{ KryoRegistrator => SparkKryoRegistrator }
import org.slf4j.LoggerFactory

import com.esotericsoftware.kryo.Kryo

import com.asakusafw.runtime.value._
import com.asakusafw.spark.runtime.driver.ShuffleKey
import com.asakusafw.spark.runtime.rdd.Branch

class KryoRegistrator extends SparkKryoRegistrator {

  import KryoRegistrator._ // scalastyle:ignore

  override def registerClasses(kryo: Kryo): Unit = {
    kryo.register(classOf[Configuration], WritableSerializer.ConfigurationSerializer)

    yarnConfSerializer.foreach {
      case (yarnConfClass, serializer) =>
        kryo.register(yarnConfClass, serializer)
    }

    kryo.register(classOf[Branch[_]], BranchSerializer)
    kryo.register(classOf[ShuffleKey], ShuffleKeySerializer)

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
