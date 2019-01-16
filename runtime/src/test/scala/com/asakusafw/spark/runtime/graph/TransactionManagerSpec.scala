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
package graph

import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner

import java.io.File

import org.apache.hadoop.conf.Configuration

import com.asakusafw.runtime.directio.hadoop.HadoopDataSource

@RunWith(classOf[JUnitRunner])
class TransactionManagerSpecTest extends TransactionManagerSpec

class TransactionManagerSpec extends FlatSpec with TempDirForEach {

  behavior of classOf[TransactionManager].getSimpleName

  it should "manage simple" in {
    val root = createTempDirectoryForEach("transaction-").toFile()
    val tm = new TransactionManager(newConfiguration(root), "testing", Map.empty)

    val a = tm.acquire("a")
    assert(tm.isCommitted() === false)

    tm.begin()
    assert(tm.isCommitted() === true)

    tm.release(a)
    assert(tm.isCommitted() === true)

    tm.end()
    assert(tm.isCommitted() === false)
  }

  it should "manage no action" in {
    val root = createTempDirectoryForEach("transaction-").toFile()
    val tm = new TransactionManager(newConfiguration(root), "testing", Map.empty)

    assert(tm.isCommitted() === false)

    tm.begin()
    assert(tm.isCommitted() === true)

    tm.end()
    assert(tm.isCommitted() === false)
  }

  it should "manage still running" in {
    val root = createTempDirectoryForEach("transaction-").toFile()
    val tm = new TransactionManager(newConfiguration(root), "testing", Map.empty)

    val a = tm.acquire("a")
    tm.acquire("orphan")
    assert(tm.isCommitted() === false)

    tm.begin()
    assert(tm.isCommitted() === true)

    tm.release(a)
    assert(tm.isCommitted() === true)

    tm.end()
    assert(tm.isCommitted() === true)
  }

  private def newConfiguration(root: File): Configuration = {
    val conf = new Configuration()
    conf.set("com.asakusafw.output.system.dir", root.getAbsolutePath)
    conf
  }
}
