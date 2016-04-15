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
package com.asakusafw.spark.runtime.orderings

import com.asakusafw.spark.runtime.rdd.ShuffleKey

class GroupingOrdering extends Ordering[ShuffleKey] {

  override def compare(left: ShuffleKey, right: ShuffleKey): Int =
    Ordering.Int.compare(left.grouping, right.grouping)
}

object GroupingOrdering extends GroupingOrdering
