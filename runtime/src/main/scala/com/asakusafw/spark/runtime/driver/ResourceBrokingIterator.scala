package com.asakusafw.spark.runtime
package driver

import org.apache.hadoop.conf.Configuration

import com.asakusafw.bridge.broker.{ ResourceBroker, ResourceSession }
import com.asakusafw.bridge.stage.StageInfo
import com.asakusafw.runtime.core.{ HadoopConfiguration, ResourceConfiguration }

class ResourceBrokingIterator[+T](val hadoopConf: Configuration, val delegate: Iterator[T])
    extends Iterator[T] {

  val session = ResourceBroker.attach(
    ResourceBroker.Scope.THREAD,
    new ResourceBroker.Initializer {
      override def accept(session: ResourceSession): Unit = {
        session.put(classOf[ResourceConfiguration], new HadoopConfiguration(hadoopConf))
        session.put(classOf[StageInfo], StageInfo.deserialize(hadoopConf.get(Props.StageInfo)))
      }
    })

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
