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
package org.apache.spark

import org.apache.spark.util.CallSite

package object backdoor {

  type HttpServer = org.apache.spark.HttpServer
  type SecurityManager = org.apache.spark.SecurityManager

  implicit class SparkContextBackdoor(val sc: SparkContext) extends AnyVal {

    def clean[F <: AnyRef](f: F, checkSerializable: Boolean = true): F = {
      sc.clean(f, checkSerializable)
    }

    def setCallSite(callSite: CallSite): Unit = sc.setCallSite(callSite)
  }
}
