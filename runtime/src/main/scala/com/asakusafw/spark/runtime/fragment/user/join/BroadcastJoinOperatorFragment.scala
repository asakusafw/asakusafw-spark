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
package com.asakusafw.spark.runtime.fragment
package user.join

import scala.collection.JavaConverters._

import com.asakusafw.runtime.core.GroupView
import com.asakusafw.runtime.model.DataModel

abstract class BroadcastJoinOperatorFragment[M <: DataModel[M], T <: DataModel[T]]
  extends Fragment[T]
  with Join[M, T] {

  protected def masters: GroupView[M]

  protected def keyElements(tx: T): Array[AnyRef]

  override def doAdd(tx: T): Unit = {
    val master = masterSelection(masters.find(keyElements(tx): _*), tx)
    join(master, tx)
  }
}

abstract class BroadcastMasterBranchOperatorFragment[M <: DataModel[M], T <: DataModel[T], E <: Enum[E]]( // scalastyle:ignore
  val children: Map[E, Fragment[T]])
  extends BroadcastJoinOperatorFragment[M, T]
  with MasterBranch[M, T, E] {

  override def doReset(): Unit = {
    children.values.foreach(_.reset())
  }
}

abstract class BroadcastMasterCheckOperatorFragment[M <: DataModel[M], T <: DataModel[T]](
  val missed: Fragment[T],
  val found: Fragment[T])
  extends BroadcastJoinOperatorFragment[M, T]
  with MasterCheck[M, T] {

  override def doReset(): Unit = {
    missed.reset()
    found.reset()
  }
}

abstract class BroadcastMasterJoinOperatorFragment[M <: DataModel[M], T <: DataModel[T], J <: DataModel[J]]( // scalastyle:ignore
  val missed: Fragment[T],
  val joined: Fragment[J],
  val joinedDataModel: J)
  extends BroadcastJoinOperatorFragment[M, T]
  with MasterJoin[M, T, J] {

  override def doReset(): Unit = {
    missed.reset()
    joined.reset()
  }
}

abstract class BroadcastMasterJoinUpdateOperatorFragment[M <: DataModel[M], T <: DataModel[T]](
  val missed: Fragment[T],
  val updated: Fragment[T])
  extends BroadcastJoinOperatorFragment[M, T]
  with MasterJoinUpdate[M, T] {

  override def doReset(): Unit = {
    missed.reset()
    updated.reset()
  }
}
