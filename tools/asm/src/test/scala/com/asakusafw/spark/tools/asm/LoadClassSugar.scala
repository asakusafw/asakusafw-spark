package com.asakusafw.spark.tools.asm

import java.io._
import java.net.{ URL, URLClassLoader }

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

  def loadClass(classname: String, classpath: File): Class[_] = {
    val classLoader = new URLClassLoader(Array(classpath.toURI.toURL), Thread.currentThread.getContextClassLoader)
    classLoader.loadClass(classname)
  }

  class SimpleClassLoader(parent: ClassLoader) extends ClassLoader(parent) {

    private val bytes = mutable.Map.empty[String, Array[Byte]]

    def put(name: String, bytes: => Array[Byte]) = {
      if (this.bytes.contains(name)) {
        this.bytes.get(name)
      } else {
        this.bytes.put(name, bytes)
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
