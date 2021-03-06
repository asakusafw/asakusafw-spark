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
package com.asakusafw.spark.runtime.fragment

import scala.reflect.{ classTag, ClassTag }

import org.apache.hadoop.io.Writable

import com.asakusafw.runtime.model.DataModel

class GenericOutputFragment[T <: DataModel[T] with Writable: ClassTag](bufferSize: Int)
  extends OutputFragment[T](bufferSize) {

  def this() = this(-1)

  override def newDataModel(): T = {
    classTag[T].runtimeClass.newInstance().asInstanceOf[T]
  }
}
