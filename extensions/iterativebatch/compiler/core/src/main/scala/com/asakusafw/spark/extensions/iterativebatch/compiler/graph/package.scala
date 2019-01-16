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
package com.asakusafw.spark.extensions.iterativebatch.compiler

import com.asakusafw.lang.compiler.planning.SubPlan
import com.asakusafw.spark.compiler.planning.{ NameInfo, SubPlanInfo }

package object graph {

  implicit class SubPlanNames(val subplan: SubPlan) extends AnyVal {

    def name: String = {
      Option(subplan.getAttribute(classOf[NameInfo]))
        .map(_.getName)
        .getOrElse("N/A")
    }

    def label: String = {
      Seq(
        Option(subplan.getAttribute(classOf[NameInfo]))
          .map(_.getName),
        Option(subplan.getAttribute(classOf[SubPlanInfo]))
          .flatMap(info => Option(info.getLabel)))
        .flatten match {
        case Seq() => "N/A"
        case s: Seq[String] => s.mkString(":")
      }
    }
  }
}
