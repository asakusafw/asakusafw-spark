package org.apache.spark.executor

package object backdoor {

  implicit class TaskMetricsBackdoor(val metrics: TaskMetrics) extends AnyVal {

    def incMemoryBytesSpilled(value: Long) = metrics.incMemoryBytesSpilled(value)
  }
}
