/*
 * Copyright 2011-2016 Asakusa Framework Team.
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

import java.util.concurrent.locks.ReentrantReadWriteLock

import scala.collection.mutable.Map

// scalastyle:off
trait ReadWriteLockedMap[A, B] extends Map[A, B] {

  private val (readLock, writeLock) = {
    val lock = new ReentrantReadWriteLock()
    (lock.readLock, lock.writeLock)
  }

  abstract override def get(key: A): Option[B] =
    readLock.acquireFor(super.get(key))
  abstract override def iterator: Iterator[(A, B)] =
    readLock.acquireFor(super.iterator)
  abstract override def +=(kv: (A, B)): this.type =
    writeLock.acquireFor(super.+=(kv))
  abstract override def -=(key: A): this.type =
    writeLock.acquireFor(super.-=(key))

  override def size: Int =
    readLock.acquireFor(super.size)
  override def put(key: A, value: B): Option[B] =
    writeLock.acquireFor(super.put(key, value))
  override def update(key: A, value: B): Unit =
    writeLock.acquireFor(super.update(key, value))
  override def remove(key: A): Option[B] =
    writeLock.acquireFor(super.remove(key))
  override def clear(): Unit =
    writeLock.acquireFor(super.clear())
  override def getOrElseUpdate(key: A, default: => B): B =
    readLock.acquireFor(super.getOrElseUpdate(key, default))
  override def transform(f: (A, B) => B): this.type =
    writeLock.acquireFor(super.transform(f))
  override def retain(p: (A, B) => Boolean): this.type =
    writeLock.acquireFor(super.retain(p))
  override def values: scala.collection.Iterable[B] =
    readLock.acquireFor(super.values)
  override def valuesIterator: Iterator[B] =
    readLock.acquireFor(super.valuesIterator)
  override def clone(): Self =
    readLock.acquireFor(super.clone())
  override def foreach[U](f: ((A, B)) => U): Unit =
    readLock.acquireFor(super.foreach(f))
  override def apply(key: A): B =
    readLock.acquireFor(super.apply(key))
  override def keySet: scala.collection.Set[A] =
    readLock.acquireFor(super.keySet)
  override def keys: scala.collection.Iterable[A] =
    readLock.acquireFor(super.keys)
  override def keysIterator: Iterator[A] =
    readLock.acquireFor(super.keysIterator)
  override def isEmpty: Boolean =
    readLock.acquireFor(super.isEmpty)
  override def contains(key: A): Boolean =
    readLock.acquireFor(super.contains(key))
  override def isDefinedAt(key: A): Boolean =
    readLock.acquireFor(super.isDefinedAt(key))
}
// scalastyle:on
