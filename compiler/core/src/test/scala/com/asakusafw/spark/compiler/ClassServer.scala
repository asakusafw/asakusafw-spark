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
import org.apache.spark.backdoor.{ HttpServer, SecurityManager }
import org.objectweb.asm.ClassReader
import org.objectweb.asm.Type
import org.objectweb.asm.util.TraceClassVisitor
import org.slf4j.LoggerFactory

import resource._

class ClassServer(val root: Path, val parent: ClassLoader, conf: SparkConf, securityManager: SecurityManager) {

  val Logger = LoggerFactory.getLogger(getClass)

  def this(parent: ClassLoader, conf: SparkConf, securityManager: SecurityManager) = {
    this(TempDir.createTempDirectory("classserver-"), parent, conf, securityManager)
  }

  def this(parent: ClassLoader, conf: SparkConf) = {
    this(parent, conf, new SecurityManager(conf))
  }

  private val httpServer = new HttpServer(conf, root.toFile(), securityManager, serverName = "ClassServer")
  val classLoader = new URLClassLoader(Array(root.toUri.toURL), parent)

  val registered = mutable.Set.empty[Type]

  def start(): String = {
    httpServer.start()
    Logger.info(s"Class server started at ${httpServer.uri}")
    httpServer.uri
  }

  def stop(): Unit = {
    httpServer.stop()
    Logger.info("Class server stopped")
  }

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
