/*
 * Copyright 2011-2017 Asakusa Framework Team.
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

import org.scalatest.{ BeforeAndAfterAll, Suite }

import scala.collection.JavaConversions._

import org.apache.spark.SparkConf

import com.asakusafw.spark.runtime.{ SparkForAll, TempDirForAll }

trait ClassServerForAll extends BeforeAndAfterAll with TempDirForAll { self: Suite =>

  var cl: ClassLoader = _
  var classServer: ClassServer = _

  override def beforeAll(): Unit = {
    try {
      super.beforeAll()
    } finally {
      cl = Thread.currentThread().getContextClassLoader()
      classServer = new ClassServer(createTempDirectoryForAll("classserver-"), cl)
      Thread.currentThread().setContextClassLoader(classServer.classLoader)
    }
  }

  override def afterAll(): Unit = {
    try {
      classServer = null
      Thread.currentThread().setContextClassLoader(cl)
      cl = null
    } finally {
      super.afterAll()
    }
  }
}
