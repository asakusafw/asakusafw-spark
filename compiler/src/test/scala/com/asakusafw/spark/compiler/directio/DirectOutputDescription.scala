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
package com.asakusafw.spark.compiler.directio

import java.util.{ List => JList }

import scala.collection.JavaConversions._
import scala.language.existentials
import scala.reflect.{ classTag, ClassTag }

import com.asakusafw.runtime.directio.DataFormat
import com.asakusafw.vocabulary.directio.DirectFileOutputDescription

case class DirectOutputDescription[T: ClassTag](
  basePath: String,
  resourcePattern: String,
  order: Seq[String],
  deletePatterns: Seq[String],
  formatType: Class[_ <: DataFormat[T]]) extends DirectFileOutputDescription {

  override def getModelType(): Class[_] = classTag[T].runtimeClass

  override def getBasePath(): String = basePath

  override def getResourcePattern(): String = resourcePattern

  override def getOrder(): JList[String] = order

  override def getDeletePatterns: JList[String] = deletePatterns

  override def getFormat = formatType
}

object DirectOutputDescription {

  def apply[T](
    basePath: String,
    resourcePattern: String,
    formatType: Class[_ <: DataFormat[T]]): DirectOutputDescription[T] = {
    DirectOutputDescription(
      basePath, resourcePattern, Seq.empty, Seq.empty, formatType)(
        ClassTag(formatType.newInstance().getSupportedType))
  }
}
