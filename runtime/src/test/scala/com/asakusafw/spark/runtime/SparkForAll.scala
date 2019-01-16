/*
 * Copyright 2011-2019 Asakusa Framework Team.
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
package com.asakusafw.spark.runtime

import org.scalatest.{ BeforeAndAfterAll, Suite }

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

trait SparkForAll extends BeforeAndAfterAll { self: Suite =>

  var sc: SparkContext = _

  def beforeStartSparkContext(): Unit = {
  }

  def configure(conf: SparkConf): SparkConf = conf

  def kryoRegistrator: String = "com.asakusafw.spark.runtime.serializer.KryoRegistrator"

  def afterStartSparkContext(): Unit = {
  }

  override def beforeAll(): Unit = {
    try {
      super.beforeAll()
    } finally {
      beforeStartSparkContext()

      val conf = new SparkConf()
      conf.setMaster("local[8]")
      conf.setAppName(getClass.getName)
      conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      conf.set("spark.kryo.registrator", kryoRegistrator)
      conf.set("spark.kryo.referenceTracking", false.toString)

      sc = SparkContext.getOrCreate(configure(conf))
      afterStartSparkContext()
    }
  }

  def beforeStopSparkContext(): Unit = {
  }

  def afterStopSparkContext(): Unit = {
  }

  override def afterAll(): Unit = {
    try {
      try {
        beforeStopSparkContext()
      } finally {
        try {
          sc.stop
          sc = null
        } finally {
          afterStopSparkContext()
        }
      }
    } finally {
      super.afterAll()
    }
  }
}
