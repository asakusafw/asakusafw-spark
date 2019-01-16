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

import org.scalatest.{ BeforeAndAfterAll, BeforeAndAfterEach, Suite }

import java.nio.file.{ Files, Path }

import scala.collection.JavaConversions._
import scala.collection.mutable

import resource._

object TempDir {

  def deleteRecursively(path: Path): Unit = {
    if (Files.exists(path)) {
      if (Files.isDirectory(path)) {
        managed(Files.newDirectoryStream(path))
          .acquireAndGet { stream =>
            stream.iterator.toList
          }
          .foreach(deleteRecursively)
      }
      Files.delete(path)
    }
  }
}

trait TempDirForAll extends BeforeAndAfterAll { self: Suite =>

  private val tmpDirs = mutable.Set.empty[Path]

  def createTempDirectoryForAll(prefix: String): Path = {
    val tmpDir = Files.createTempDirectory(prefix)
    tmpDirs += tmpDir
    tmpDir
  }

  override def afterAll(): Unit = {
    try {
      tmpDirs.foreach(TempDir.deleteRecursively)
      tmpDirs.clear()
    } finally {
      super.afterAll()
    }
  }
}

trait TempDirForEach extends BeforeAndAfterEach { self: Suite =>

  private val tmpDirs = mutable.Set.empty[Path]

  def createTempDirectoryForEach(prefix: String): Path = {
    val tmpDir = Files.createTempDirectory(prefix)
    tmpDirs += tmpDir
    tmpDir
  }

  override def afterEach(): Unit = {
    try {
      tmpDirs.foreach(TempDir.deleteRecursively)
      tmpDirs.clear()
    } finally {
      super.afterEach()
    }
  }
}
