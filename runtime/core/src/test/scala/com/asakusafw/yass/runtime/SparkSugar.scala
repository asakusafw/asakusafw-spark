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
package com.asakusafw.yass.runtime

import org.scalatest.BeforeAndAfterAll
import org.scalatest.Suite

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

trait SparkSugar extends BeforeAndAfterAll {
  self: Suite =>

  var sc: SparkContext = _

  override def beforeAll() {
    try {
      super.beforeAll()
    } finally {
      val conf = new SparkConf()
      conf.setMaster("local[*]")
      conf.setAppName(getClass.getName)
      conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      conf.set("spark.kryo.registrator", kryoRegistrator)

      sc = new SparkContext(conf)
    }
  }

  def kryoRegistrator: String = "com.asakusafw.spark.runtime.serializer.KryoRegistrator"

  override def afterAll() {
    try {
      sc.stop
      sc = null
    } finally {
      super.afterAll()
    }
  }
}
