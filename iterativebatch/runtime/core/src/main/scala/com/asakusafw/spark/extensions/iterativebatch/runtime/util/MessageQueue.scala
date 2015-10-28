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
package com.asakusafw.spark.extensions.iterativebatch.runtime.util

import java.util.concurrent.{ Executors, TimeUnit }
import java.util.concurrent.locks.{ Condition, Lock, ReentrantReadWriteLock }

import scala.annotation.tailrec
import scala.collection.mutable
import scala.concurrent.duration.Duration

import com.google.common.util.concurrent.ThreadFactoryBuilder

abstract class MessageQueue[M](name: String, numThreads: Int = 1, numSlots: Int = Int.MaxValue) {

  require(numThreads > 0, s"The number of threads should be greater than 0: [${numThreads}].")
  require(numSlots > 0, s"The number of slots should be greater than 0: [${numSlots}].")

  private val (readLock, writeLock) = {
    val lock = new ReentrantReadWriteLock()
    (lock.readLock, lock.writeLock)
  }

  def started: Boolean = readLock.acquireFor(running || terminating)

  private var _running: Boolean = false
  def running: Boolean = readLock.acquireFor(_running)

  private var _terminating: Boolean = false
  def terminating: Boolean = readLock.acquireFor(_terminating)

  private var _stopped: Boolean = false
  def stopped: Boolean = readLock.acquireFor(_stopped)

  private var _stoppedThreads: Int = 0

  private val queue = mutable.Queue.empty[M]
  def size: Int = readLock.acquireFor(queue.size)

  private val handlingMessages = mutable.Set.empty[M]
  def numHandlingMessages: Int = readLock.acquireFor(handlingMessages.size)

  private val queueIsAvailable = writeLock.newCondition()
  private val executeCompletion = writeLock.newCondition()
  private val termination = writeLock.newCondition()

  private val executor =
    Executors.newFixedThreadPool(
      numThreads,
      new ThreadFactoryBuilder()
        .setDaemon(true)
        .setNameFormat(if (numThreads > 1) s"${name}-%d" else name)
        .build())

  private val runnable = new Runnable {

    @tailrec
    override def run(): Unit = {
      writeLock.acquireFor {
        @tailrec
        def dequeueOrTerminate(): Option[M] = {
          if (running) {
            if (queue.isEmpty) {
              queueIsAvailable.await()
              dequeueOrTerminate()
            } else if (handlingMessages.size >= numSlots) {
              executeCompletion.await()
              dequeueOrTerminate()
            } else {
              val message = queue.dequeue()
              handlingMessages += message
              Some(message)
            }
          } else {
            _stoppedThreads += 1
            termination.signalAll()
            None
          }
        }
        dequeueOrTerminate()
      } match {
        case Some(message) =>
          handleMessage(message) { () =>
            writeLock.acquireFor {
              handlingMessages -= message
              executeCompletion.signalAll()
            }
          }
          run()
        case _ =>
      }
    }
  }

  protected def handleMessage(message: M)(onComplete: () => Unit): Unit

  protected def onStart(): Unit = {}
  protected def onStop(): Unit = {}

  def start(): Unit = {
    writeLock.acquireFor {
      if (!started && !stopped) {
        onStart()

        _running = true
        (0 until numThreads).foreach(_ => executor.execute(runnable))
      } else {
        throw new IllegalStateException(s"${name} has already been started.")
      }
    }
  }

  def submit(message: M): Unit = {
    writeLock.acquireFor {
      if (!terminating && !stopped) {
        queue += message
        queueIsAvailable.signalAll()
      } else {
        throw new IllegalStateException(s"${name} is terminating or stopped.")
      }
    }
  }

  def submitAll(messages: Seq[M]): Unit = {
    writeLock.acquireFor {
      if (!terminating && !stopped) {
        queue ++= messages
        queueIsAvailable.signalAll()
      } else {
        throw new IllegalStateException(s"${name} is terminating or stopped.")
      }
    }
  }

  def awaitExecution(): Unit = {
    awaitExecution(Duration.Inf)
  }

  def awaitExecution(timeout: Long, unit: TimeUnit): Boolean = {
    awaitExecution(Duration(timeout, unit))
  }

  def awaitExecution(duration: Duration): Boolean = {
    val remain = duration.remainFrom(System.currentTimeMillis())

    writeLock.tryAcquireFor(remain()) {
      if (running) {
        @tailrec
        def await(): Boolean = {
          if (queue.isEmpty && handlingMessages.isEmpty) {
            true
          } else if (executeCompletion.awaitFor(remain())) {
            await()
          } else {
            false
          }
        }
        await()
      } else {
        throw new IllegalStateException(s"${name} is not running.")
      }
    }
  }

  def stop(awaitExecution: Boolean = false, gracefully: Boolean = false): Seq[M] = {
    writeLock.acquireFor {
      if (!terminating || !stopped) {
        _terminating = true
        if (awaitExecution) {
          this.awaitExecution()
        }
        _running = false
        queueIsAvailable.signalAll()
        if (gracefully) {
          awaitTermination()
        }
        executor.shutdown()
        _terminating = false
        _stopped = true

        onStop()
      }
      queue.toSeq
    }
  }

  def awaitTermination(): Unit = {
    awaitTermination(Duration.Inf)
  }

  def awaitTermination(timeout: Long, unit: TimeUnit): Boolean = {
    awaitTermination(Duration(timeout, unit))
  }

  def awaitTermination(duration: Duration): Boolean = {
    val remain = duration.remainFrom(System.currentTimeMillis())

    writeLock.tryAcquireFor(remain()) {
      if (running || terminating) {
        @tailrec
        def await(): Boolean = {
          if (_stoppedThreads == numThreads) {
            true
          } else if (termination.awaitFor(remain())) {
            await()
          } else {
            false
          }
        }
        await()
      } else {
        throw new IllegalStateException(s"${name} is not running.")
      }
    }
  }
}
