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

import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.{ Condition, Lock }

import scala.concurrent.duration.Duration

package object util {

  implicit class AugmentedLock(val lock: Lock) extends AnyVal {

    def acquireFor[A](block: => A): A = {
      lock.lock()
      try {
        block
      } finally {
        lock.unlock()
      }
    }

    def tryAcquireFor[A](duration: Duration)(block: => Boolean): Boolean = {
      if (duration.isFinite) {
        val locked = lock.tryLock(duration.length, duration.unit)
        if (locked) {
          try {
            block
          } finally {
            lock.unlock()
          }
        } else {
          false
        }
      } else {
        acquireFor(block)
      }
    }
  }

  implicit class AugmentedCondition(val cond: Condition) extends AnyVal {

    def awaitFor(duration: Duration): Boolean = {
      if (duration.isFinite) {
        cond.await(duration.length, duration.unit)
      } else {
        cond.await()
        true
      }
    }
  }

  implicit class AugmentedDuration(val duration: Duration) extends AnyVal {

    def remainFrom(from: Long): () => Duration = {
      if (duration.isFinite) {
        val until = from + duration.toMillis
        () => Duration(until - System.currentTimeMillis(), TimeUnit.MILLISECONDS)
      } else {
        () => Duration.Inf
      }
    }
  }
}
