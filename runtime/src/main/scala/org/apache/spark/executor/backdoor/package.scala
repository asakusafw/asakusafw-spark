/*
 * Copyright 2011-2017 Asakusa Framework Team.
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
package org.apache.spark.executor

package object backdoor {

  implicit class TaskMetricsBackdoor(val metrics: TaskMetrics) extends AnyVal {

    def incMemoryBytesSpilled(value: Long): Unit = metrics.incMemoryBytesSpilled(value)
    def incDiskBytesSpilled(value: Long): Unit = metrics.incDiskBytesSpilled(value)
  }

  implicit class InputMetricsBackdoor(val metrics: InputMetrics) extends AnyVal {

    def incBytesRead(v: Long): Unit = metrics.incBytesRead(v)
    def incRecordsRead(v: Long): Unit = metrics.incRecordsRead(v)
    def setBytesRead(v: Long): Unit = metrics.setBytesRead(v)
  }

  implicit class OutputMetricsBackdoor(val metrics: OutputMetrics) extends AnyVal {

    def setBytesWritten(v: Long): Unit = metrics.setBytesWritten(v)
    def setRecordsWritten(v: Long): Unit = metrics.setRecordsWritten(v)
  }
}
