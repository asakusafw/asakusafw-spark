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
package com.asakusafw.spark.runtime
package graph

import scala.collection.JavaConversions._

import org.apache.hadoop.conf.Configuration

import com.asakusafw.bridge.api.activate.ApiActivator
import com.asakusafw.bridge.broker.{ ResourceBroker, ResourceSession }
import com.asakusafw.bridge.stage.StageInfo
import com.asakusafw.runtime.core.{ HadoopConfiguration, ResourceConfiguration }

class ResourceBrokingIterator[+T](val hadoopConf: Configuration, _delegate: => Iterator[T])
  extends Iterator[T] {

  val _ = ResourceBrokingIterator.activators // Initialize activators.

  val session = ResourceBroker.attach(
    ResourceBroker.Scope.THREAD,
    new ResourceBroker.Initializer {
      override def accept(session: ResourceSession): Unit = {
        session.put(classOf[ResourceConfiguration], new HadoopConfiguration(hadoopConf))
        session.put(classOf[StageInfo], StageInfo.deserialize(hadoopConf.get(StageInfo.KEY_NAME)))
      }
    })

  val delegate = _delegate

  def hasNext: Boolean = {
    if (delegate.hasNext) {
      true
    } else {
      session.close()
      false
    }
  }

  def next(): T = delegate.next()
}

object ResourceBrokingIterator {

  val activators = ApiActivator.load(Thread.currentThread.getContextClassLoader).map(_.activate)
}
