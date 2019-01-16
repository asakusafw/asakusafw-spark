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
package com.asakusafw.spark.extensions.iterativebatch.runtime.graph

import scala.concurrent.{ ExecutionContext, Future }

import org.slf4j.LoggerFactory

import com.asakusafw.bridge.stage.StageInfo
import com.asakusafw.runtime.directio.{ Counter, FilePattern }
import com.asakusafw.runtime.directio.hadoop.HadoopDataSourceUtil
import com.asakusafw.spark.runtime.{ JobContext, RoundContext }
import com.asakusafw.spark.runtime.graph._

abstract class DirectOutputSetupForIterative(
  setup: DirectOutputSetup)(
    implicit val jobContext: JobContext) extends IterativeAction[Unit] {
  self: CacheStrategy[Seq[RoundContext], Future[Unit]] =>

  override val label = getClass.getSimpleName

  override protected def doPerform(
    origin: RoundContext,
    rcs: Seq[RoundContext])(implicit ec: ExecutionContext): Future[Unit] = {
    Future.sequence(rcs.map(setup.perform)).map(_ => ())
  }
}
