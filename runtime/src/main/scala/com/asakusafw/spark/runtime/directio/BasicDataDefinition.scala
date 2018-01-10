/*
 * Copyright 2011-2018 Asakusa Framework Team.
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
package com.asakusafw.spark.runtime.directio

import com.asakusafw.runtime.directio.{ DataDefinition, DataFilter, DataFormat }

case class BasicDataDefinition[T] private (
  dataFormat: DataFormat[T],
  dataFilter: Option[DataFilter[_ >: T]])
  extends DataDefinition[T] {

  override def getDataClass(): Class[_ <: T] = dataFormat.getSupportedType

  override def getDataFormat(): DataFormat[T] = dataFormat

  override def getDataFilter: DataFilter[_ >: T] = dataFilter.orNull
}

object BasicDataDefinition {

  def apply[T](dataFormat: DataFormat[T]): DataDefinition[T] = {
    BasicDataDefinition(dataFormat, None)
  }

  def apply[T](dataFormat: DataFormat[T], dataFilter: DataFilter[_]): DataDefinition[T] = {
    BasicDataDefinition[T](dataFormat, Some(dataFilter.asInstanceOf[DataFilter[_ >: T]]))
  }

  def apply[T](
    factory: ObjectFactory,
    dataFormatClass: Class[_ <: DataFormat[T]]): DataDefinition[T] = {
    BasicDataDefinition[T](factory.newInstance(dataFormatClass), None)
  }

  def apply[T](
    factory: ObjectFactory,
    dataFormatClass: Class[_ <: DataFormat[T]],
    dataFilterClass: Option[Class[_ <: DataFilter[_]]]): DataDefinition[T] = {
    BasicDataDefinition[T](
      factory.newInstance(dataFormatClass),
      dataFilterClass.map(factory.newInstance(_).asInstanceOf[DataFilter[_ >: T]]))
  }
}
