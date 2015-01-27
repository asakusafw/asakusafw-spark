package com.asakusafw.spark.tools.asm

import java.io.PrintWriter
import java.io.StringWriter

import scala.collection.mutable

import org.objectweb.asm.ClassReader
import org.objectweb.asm.Opcodes._
import org.objectweb.asm.util.TraceClassVisitor
import org.slf4j.LoggerFactory

trait LoadClassSugar {

  val logger = LoggerFactory.getLogger(getClass)

  def loadClass(classname: String, bytes: Array[Byte]): Class[_] = {
    val classLoader = new SimpleClassLoader(Thread.currentThread.getContextClassLoader)
    classLoader.put(classname, bytes)
    classLoader.loadClass(classname)
  }

  class SimpleClassLoader(parent: ClassLoader) extends ClassLoader(parent) {

    private val bytes = mutable.Map.empty[String, Array[Byte]]

    def put(name: String, bytes: => Array[Byte]) = {
      if (this.bytes.contains(name)) {
        this.bytes.get(name)
      } else {
        val bs = bytes
        if (logger.isInfoEnabled) {
          logger.info {
            val writer = new StringWriter
            val cr = new ClassReader(bs)
            cr.accept(new TraceClassVisitor(new PrintWriter(writer)), 0)
            writer.toString
          }
        }
        this.bytes.put(name, bs)
      }
    }

    override def loadClass(name: String, resolve: Boolean): Class[_] = {
      Option(findLoadedClass(name)).getOrElse {
        bytes.get(name).map { bytes =>
          defineClass(name, bytes, 0, bytes.length)
        }.getOrElse(super.loadClass(name, resolve))
      }
    }
  }
}
