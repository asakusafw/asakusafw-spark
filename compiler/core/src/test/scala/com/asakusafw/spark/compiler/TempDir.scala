package com.asakusafw.spark.compiler

import java.io.File
import java.nio.file.{ Files, Path }

import scala.collection.JavaConversions._

trait TempDir {

  def createTempDirectory(prefix: String): Path = {
    val tmpDir = Files.createTempDirectory(prefix)
    sys.addShutdownHook {
      def deleteRecursively(path: Path): Unit = {
        if (Files.isDirectory(path)) {
          Files.newDirectoryStream(path).foreach(deleteRecursively)
        }
        Files.delete(path)
      }
      deleteRecursively(tmpDir)
    }
    tmpDir
  }
}

object TempDir extends TempDir
