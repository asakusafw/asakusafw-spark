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

import com.asakusafw.runtime.model.DataModel

abstract class BroadcastJoinOperatorFragment[K, M <: DataModel[M], T <: DataModel[T]](
  masters: Map[K, Seq[M]])
  extends Fragment[T]
  with Join[M, T] {

  def key(tx: T): K

  override def doAdd(tx: T): Unit = {
    val k = key(tx)
    val master = masterSelection(masters.getOrElse(k, Seq.empty[M]).asJava, tx)
    join(master, tx)
  }
}

abstract class BroadcastMasterBranchOperatorFragment[K, M <: DataModel[M], T <: DataModel[T], E <: Enum[E]]( // scalastyle:ignore
  masters: Map[K, Seq[M]])(
    val children: Map[E, Fragment[T]])
  extends BroadcastJoinOperatorFragment[K, M, T](masters)
  with MasterBranch[M, T, E] {

  override def doReset(): Unit = {
    children.values.foreach(_.reset())
  }
}

abstract class BroadcastMasterCheckOperatorFragment[K, M <: DataModel[M], T <: DataModel[T]](
  masters: Map[K, Seq[M]])(
    val missed: Fragment[T],
    val found: Fragment[T])
  extends BroadcastJoinOperatorFragment[K, M, T](masters)
  with MasterCheck[M, T] {

  override def doReset(): Unit = {
    missed.reset()
    found.reset()
  }
}

abstract class BroadcastMasterJoinOperatorFragment[K, M <: DataModel[M], T <: DataModel[T], J <: DataModel[J]]( // scalastyle:ignore
  masters: Map[K, Seq[M]])(
    val missed: Fragment[T],
    val joined: Fragment[J],
    val joinedDataModel: J)
  extends BroadcastJoinOperatorFragment[K, M, T](masters)
  with MasterJoin[M, T, J] {

  override def doReset(): Unit = {
    missed.reset()
    joined.reset()
  }
}

abstract class BroadcastMasterJoinUpdateOperatorFragment[K, M <: DataModel[M], T <: DataModel[T]](
  masters: Map[K, Seq[M]])(
    val missed: Fragment[T],
    val updated: Fragment[T])
  extends BroadcastJoinOperatorFragment[K, M, T](masters)
  with MasterJoinUpdate[M, T] {

  override def doReset(): Unit = {
    missed.reset()
    updated.reset()
  }
}
