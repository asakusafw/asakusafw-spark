/*
 * Copyright 2011-2021 Asakusa Framework Team.
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

import scala.concurrent.{ ExecutionContext, Future }

import org.slf4j.LoggerFactory

import com.asakusafw.bridge.stage.StageInfo
import com.asakusafw.runtime.directio.{ Counter, FilePattern }
import com.asakusafw.runtime.directio.hadoop.HadoopDataSourceUtil

abstract class DirectOutputSetup(
  implicit val jobContext: JobContext) extends Action[Unit] {
  self: CacheStrategy[RoundContext, Future[Unit]] =>

  private val Logger = LoggerFactory.getLogger(getClass)

  override val label = getClass.getSimpleName

  def specs: Set[(String, String, Seq[String])]

  override protected def doPerform(
    rc: RoundContext)(implicit ec: ExecutionContext): Future[Unit] = {
    val conf = rc.hadoopConf.value
    val stageInfo = StageInfo.deserialize(conf.get(StageInfo.KEY_NAME))
    val repository = HadoopDataSourceUtil.loadRepository(conf)

    Future.sequence {
      specs.collect {
        case (id, bp, deletePatterns) if deletePatterns.nonEmpty =>
          val basePath = stageInfo.resolveUserVariables(bp)
          val containerPath = repository.getContainerPath(basePath)
          val componentPath = repository.getComponentPath(basePath)
          val source = repository.getRelatedDataSource(containerPath)
          val targets = deletePatterns
            .map(stageInfo.resolveUserVariables)
            .map(FilePattern.compile)
          val counter = new Counter()
          Future {
            if (Logger.isDebugEnabled) {
              Logger.debug(s"preparing Direct I/O file output: Spec(id=${id}, basePath=${bp})")
            }
            targets.foreach { target =>
              source.delete(componentPath, target, true, counter)
            }
          }
      }
    }.map(_ => ())
  }
}
