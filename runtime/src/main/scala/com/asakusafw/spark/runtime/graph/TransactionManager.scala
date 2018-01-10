/*
 * Copyright 2011-2018 Asakusa Framework Team.
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
package com.asakusafw.spark.runtime.graph

import java.io.{ OutputStream, OutputStreamWriter, PrintWriter }
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.mutable

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.slf4j.LoggerFactory

import com.asakusafw.runtime.directio.{ Counter, OutputTransactionContext }
import com.asakusafw.runtime.directio.hadoop.HadoopDataSourceUtil

import resource._

class TransactionManager(
  configuration: Configuration,
  transactionId: String,
  properties: Map[String, String]) {

  private val Logger = LoggerFactory.getLogger(getClass)

  private val transactionProperties = if (properties.isEmpty) {
    Map("Transaction ID" -> transactionId)
  } else {
    properties
  }

  private val running: mutable.Map[String, OutputTransactionContext] = mutable.Map.empty

  def acquire(id: String): OutputTransactionContext = synchronized {
    val acquired = running.get(id)
    if (acquired.isDefined) {
      throw new IllegalStateException()
    }
    val created = new OutputTransactionContext(transactionId, id, new Counter())
    running.put(id, created)
    created
  }

  def release(context: OutputTransactionContext): Unit = synchronized {
    val id = context.getOutputId
    val acquired = running.get(id)
    if (acquired.isEmpty) {
      if (Logger.isWarnEnabled) {
        Logger.warn(s"invalid transaction output ID: ${id}")
      }
    }
    running.remove(id)
  }

  def begin(): Unit = {
    if (Logger.isDebugEnabled) {
      Logger.debug(s"starting transaction of Direct I/O file output: ${transactionId}")
    }
    setCommitted(true)
    setTransactionInfo(true)
  }

  def end(): Unit = {
    if (running.isEmpty) {
      if (Logger.isDebugEnabled) {
        Logger.debug(s"finishing transaction of Direct I/O file output: ${transactionId}")
      }
      if (isCommitted()) {
        setCommitted(false)
        setTransactionInfo(false)
      }
    }
  }

  def isCommitted(): Boolean = {
    val commitMark = getCommitMarkPath()
    val fs = commitMark.getFileSystem(configuration)
    fs.exists(commitMark)
  }

  private def setTransactionInfo(value: Boolean): Unit = {
    val transactionInfo = getTransactionInfoPath()
    val fs = transactionInfo.getFileSystem(configuration)
    if (value) {
      for {
        output <- managed(
          new TransactionManager.SafeOutputStream(fs.create(transactionInfo, false)))
        writer <- managed(
          new PrintWriter(new OutputStreamWriter(output, HadoopDataSourceUtil.COMMENT_CHARSET)))
      } {
        transactionProperties.foreach {
          case (key, value) if value != null => // scalastyle:ignore
            writer.println(s"${key}: ${value}")
          case _ =>
        }
      }
    } else {
      fs.delete(transactionInfo, false)
    }
  }

  private def setCommitted(value: Boolean): Unit = {
    val commitMark = getCommitMarkPath()
    val fs = commitMark.getFileSystem(configuration)
    if (value) {
      fs.create(commitMark, false).close()
    } else {
      fs.delete(commitMark, false)
    }
  }

  private def getTransactionInfoPath(): Path = {
    HadoopDataSourceUtil.getTransactionInfoPath(configuration, transactionId)
  }

  private def getCommitMarkPath(): Path = {
    HadoopDataSourceUtil.getCommitMarkPath(configuration, transactionId)
  }
}

object TransactionManager {

  private[TransactionManager] class SafeOutputStream(delegate: OutputStream) extends OutputStream {

    private val closed = new AtomicBoolean()

    override def write(b: Int): Unit = {
      delegate.write(b)
    }

    override def write(b: Array[Byte], off: Int, len: Int): Unit = {
      delegate.write(b, off, len)
    }

    override def close(): Unit = {
      if (closed.compareAndSet(false, true)) {
        delegate.close()
      }
    }
  }
}
