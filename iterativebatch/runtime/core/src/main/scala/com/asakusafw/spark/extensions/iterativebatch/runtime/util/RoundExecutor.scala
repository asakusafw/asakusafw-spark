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

abstract class RoundExecutor[C](name: String, numThreads: Int = 1, numSlots: Int = Int.MaxValue) {

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

  private val queue = mutable.Queue.empty[C]
  def queueSize: Int = readLock.acquireFor(queue.size)

  private val runningRounds = mutable.Set.empty[C]
  def numRunningRounds: Int = readLock.acquireFor(runningRounds.size)

  private val queueIsAvailable = writeLock.newCondition()
  private val roundCompletion = writeLock.newCondition()
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
        def dequeueOrTerminate(): Option[C] = {
          if (running) {
            if (queue.isEmpty) {
              queueIsAvailable.await()
              dequeueOrTerminate()
            } else if (runningRounds.size >= numSlots) {
              roundCompletion.await()
              dequeueOrTerminate()
            } else {
              val context = queue.dequeue()
              runningRounds += context
              Some(context)
            }
          } else {
            _stoppedThreads += 1
            termination.signalAll()
            None
          }
        }
        dequeueOrTerminate()
      } match {
        case Some(context) =>
          executeRound(context) { () =>
            writeLock.acquireFor {
              runningRounds -= context
              roundCompletion.signalAll()
            }
          }
          run()
        case _ =>
      }
    }
  }

  protected def executeRound(context: C)(onComplete: () => Unit): Unit

  def onStart(): Unit = {}
  def onStop(): Unit = {}

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

  def submit(context: C): Unit = {
    writeLock.acquireFor {
      if (!terminating && !stopped) {
        queue += context
        queueIsAvailable.signalAll()
      } else {
        throw new IllegalStateException(s"${name} is terminating or stopped.")
      }
    }
  }

  def submitAll(contexts: Seq[C]): Unit = {
    writeLock.acquireFor {
      if (!terminating && !stopped) {
        queue ++= contexts
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
          if (queue.isEmpty && runningRounds.isEmpty) {
            true
          } else if (roundCompletion.awaitFor(remain())) {
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

  def stop(awaitExecution: Boolean = false, gracefully: Boolean = false): Seq[C] = {
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
