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
package com.asakusafw.spark.runtime.fragment
package user

import com.asakusafw.runtime.model.DataModel

abstract class ConvertOperatorFragment[T <: DataModel[T], U <: DataModel[U]](
  original: Fragment[T], converted: Fragment[U])
  extends Fragment[T] {

  override def doAdd(result: T): Unit = {
    converted.add(convert(result))
    original.add(result)
  }

  def convert(input: T): U

  override def doReset(): Unit = {
    original.reset()
    converted.reset()
  }
}
