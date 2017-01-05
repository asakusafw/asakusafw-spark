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
package com.asakusafw.spark.runtime.fragment
package user

import com.asakusafw.runtime.core.Result
import com.asakusafw.runtime.model.DataModel

abstract class SplitOperatorFragment[T <: DataModel[T], L <: DataModel[L], R <: DataModel[R]](
  left: L, right: R, l: Fragment[L], r: Fragment[R])
  extends Fragment[T] {

  override def doAdd(result: T): Unit = {
    left.reset()
    right.reset()
    split(result, left, right)
    l.add(left)
    r.add(right)
  }

  def split(input: T, left: L, right: R): Unit

  override def doReset(): Unit = {
    l.reset()
    r.reset()
  }
}
