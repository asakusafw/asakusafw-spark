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

import com.asakusafw.runtime.core.Result
import com.asakusafw.runtime.model.DataModel

abstract class UpdateOperatorFragment[T <: DataModel[T]](
  child: Fragment[T])
  extends Fragment[T] {

  override def doAdd(result: T): Unit = {
    update(result)
    child.add(result)
  }

  def update(input: T): Unit

  override def doReset(): Unit = {
    child.reset()
  }
}
