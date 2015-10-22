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
package com.asakusafw.spark.extensions.iterativebatch.runtime

import java.util.concurrent.{ Executors, TimeUnit }
import java.util.concurrent.locks.{ Condition, Lock, ReentrantReadWriteLock }

import scala.annotation.tailrec
import scala.collection.mutable
import scala.concurrent.duration.Duration

import com.google.common.util.concurrent.ThreadFactoryBuilder

abstract class RoundExecutor[C](name: String, numThreads: Int, slots: Int) {

  def this(name: String, slots: Int) = this(name, 1, slots)

  def this(name: String) = this(name, Int.MaxValue)

  require(slots > 0, s"The slots should be greater than 0: [${slots}].")

  def started: Boolean = withLock(readLock)(running || terminating)

  private var _running: Boolean = false
  def running: Boolean = withLock(readLock)(_running)

  private var _terminating: Boolean = false
  def terminating: Boolean = withLock(readLock)(_terminating)

  private var _stopped: Boolean = false
  def stopped: Boolean = withLock(readLock)(_stopped)

  private val queue = mutable.Queue.empty[C]
  def queueSize: Int = withLock(readLock)(queue.size)

  private val runningRounds = mutable.Set.empty[C]
  def numRunningRounds: Int = withLock(readLock)(runningRounds.size)

  private val (readLock, writeLock) = {
    val lock = new ReentrantReadWriteLock()
    (lock.readLock, lock.writeLock)
  }

  private val queueIsAvailable = writeLock.newCondition()
  private val slotIsAvailable = writeLock.newCondition()

  private val roundStart = writeLock.newCondition()
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
      withLock(writeLock) {
        if (running) {
          if (queue.isEmpty) {
            queueIsAvailable.await()
          } else if (runningRounds.size >= slots) {
            slotIsAvailable.await()
          } else {
            val context = queue.dequeue()
            runningRounds += context
            roundStart.signalAll()

            executeRound(context) {
              withLock(writeLock) {
                runningRounds -= context
                slotIsAvailable.signalAll()

                roundCompletion.signalAll()
              }
            }
          }
          true
        } else {
          termination.signalAll()
          false
        }
      } match {
        case true => run()
        case _ =>
      }
    }
  }

  protected def executeRound(context: C)(onComplete: => Unit): Unit

  def onStart(): Unit = {}
  def onStop(): Unit = {}

  def start(): Unit = {
    withLock(writeLock) {
      if (!started && !stopped) {
        onStart()

        _running = true
        executor.execute(runnable)
      } else {
        throw new IllegalStateException(s"${name} has already been started.")
      }
    }
  }

  def submit(context: C): Unit = {
    withLock(writeLock) {
      if (!terminating && !stopped) {
        queue += context
        queueIsAvailable.signalAll()
      } else {
        throw new IllegalStateException(s"${name} is terminating or stopped.")
      }
    }
  }

  def submitAll(contexts: Seq[C]): Unit = {
    withLock(writeLock) {
      contexts.foreach(submit)
    }
  }

  def awaitExecution(): Unit = {
    awaitExecution(Duration.Inf)
  }

  def awaitExecution(timeout: Long, unit: TimeUnit): Boolean = {
    awaitExecution(Duration(timeout, unit))
  }

  def awaitExecution(duration: Duration): Boolean = {
    val remain = if (duration.isFinite) {
      val until = System.currentTimeMillis() + duration.toMillis
      () => Duration(until - System.currentTimeMillis(), TimeUnit.MILLISECONDS)
    } else {
      () => Duration.Inf
    }

    @tailrec
    def await(): Boolean = {
      withTryLock(writeLock, remain()) { locked =>
        if (locked) {
          if (running) {
            if (queue.nonEmpty) {
              if (awaitCondition(roundStart, remain())) {
                None
              } else {
                Some(false)
              }
            } else if (runningRounds.nonEmpty) {
              if (awaitCondition(roundCompletion, remain())) {
                None
              } else {
                Some(false)
              }
            } else {
              Some(true)
            }
          } else {
            throw new IllegalStateException(s"${name} is not running.")
          }
        } else {
          Some(false)
        }
      } match {
        case Some(b) => b
        case None => await()
      }
    }

    await()
  }

  def stop(awaitExecution: Boolean = false, gracefully: Boolean = false): Seq[C] = {
    withLock(writeLock) {
      if (!terminating || !stopped) {
        _terminating = true
        if (awaitExecution) {
          this.awaitExecution()
        }
        _running = false
        queueIsAvailable.signalAll()
        slotIsAvailable.signalAll()
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
    withTryLock(writeLock, duration) { locked =>
      if (locked) {
        if (running || terminating) {
          awaitCondition(termination, duration)
        } else {
          throw new IllegalStateException(s"${name} is not running.")
        }
      } else {
        false
      }
    }
  }

  private def withLock[A](lock: Lock)(block: => A): A = {
    lock.lock()
    try {
      block
    } finally {
      lock.unlock()
    }
  }

  private def withTryLock[A](lock: Lock, duration: Duration)(block: Boolean => A): A = {
    if (duration.isFinite) {
      val locked = lock.tryLock(duration.length, duration.unit)
      if (locked) {
        try {
          block(locked)
        } finally {
          lock.unlock()
        }
      } else {
        block(locked)
      }
    } else {
      withLock(lock)(block(true))
    }
  }

  private def awaitCondition(cond: Condition, duration: Duration): Boolean = {
    if (duration.isFinite) {
      cond.await(duration.length, duration.unit)
    } else {
      cond.await()
      true
    }
  }
}
