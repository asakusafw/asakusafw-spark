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
package com.asakusafw.spark.compiler

import java.io.File
import java.io.PrintWriter
import java.io.StringWriter
import java.net.URLClassLoader
import java.nio.file.Files
import java.nio.file.Path

import scala.collection.JavaConversions._
import scala.collection.mutable

import org.apache.spark.SparkConf
import org.objectweb.asm.ClassReader
import org.objectweb.asm.Type
import org.objectweb.asm.util.TraceClassVisitor
import org.slf4j.LoggerFactory

import resource._

class ClassServer(val root: Path, val parent: ClassLoader) {

  private val Logger = LoggerFactory.getLogger(getClass)

  val classLoader = new URLClassLoader(Array(root.toUri.toURL), parent)

  val registered = mutable.Set.empty[Type]

  def register(`type`: Type, bytes: => Array[Byte]): Unit = {
    if (registered(`type`)) {
      Logger.warn(s"Class[${`type`.getClassName()}] was already registered.")
    } else {
      val bs = bytes
      if (Logger.isInfoEnabled) {
        Logger.info {
          val writer = new StringWriter
          val cr = new ClassReader(bs)
          cr.accept(new TraceClassVisitor(new PrintWriter(writer)), 0)
          writer.toString
        }
      }
      val path = new File(root.toFile(), `type`.getInternalName() + ".class").toPath
      Files.createDirectories(path.getParent())
      for (os <- managed(Files.newOutputStream(path))) {
        os.write(bs)
      }
      registered += `type`
      Logger.info(s"Class[${`type`.getClassName()}] was registered.")
    }
  }

  def loadClass(`type`: Type, bytes: => Array[Byte]): Class[_] = {
    register(`type`, bytes)
    loadClass(`type`)
  }

  def loadClass(`type`: Type): Class[_] = {
    loadClass(`type`.getClassName())
  }

  def loadClass(name: String): Class[_] = {
    classLoader.loadClass(name)
  }
}
