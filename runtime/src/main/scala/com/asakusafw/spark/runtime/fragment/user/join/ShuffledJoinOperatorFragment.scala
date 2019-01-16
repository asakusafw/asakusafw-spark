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
package user.join

import com.asakusafw.runtime.flow.{ ArrayListBuffer, ListBuffer }
import com.asakusafw.runtime.model.DataModel

abstract class ShuffledJoinOperatorFragment[M <: DataModel[M], T <: DataModel[T]]
  extends Fragment[IndexedSeq[Iterator[_]]]
  with Join[M, T] {

  def newMasterDataModel: M

  private val masters: ListBuffer[M] = new ArrayListBuffer[M]()

  override def doAdd(result: IndexedSeq[Iterator[_]]): Unit = {
    masters.begin()
    result(0).asInstanceOf[Iterator[M]].foreach { master =>
      if (masters.isExpandRequired()) {
        masters.expand(newMasterDataModel)
      }
      masters.advance().copyFrom(master)
    }
    masters.end()

    result(1).asInstanceOf[Iterator[T]].foreach { tx =>
      val master = masterSelection(masters, tx)
      join(master, tx)
    }

    masters.shrink()
  }
}

abstract class ShuffledMasterBranchOperatorFragment[M <: DataModel[M], T <: DataModel[T], E <: Enum[E]]( // scalastyle:ignore
  val children: Map[E, Fragment[T]])
  extends ShuffledJoinOperatorFragment[M, T]
  with MasterBranch[M, T, E] {

  override def doReset(): Unit = {
    children.values.foreach(_.reset())
  }
}

abstract class ShuffledMasterCheckOperatorFragment[M <: DataModel[M], T <: DataModel[T]](
  val missed: Fragment[T],
  val found: Fragment[T])
  extends ShuffledJoinOperatorFragment[M, T]
  with MasterCheck[M, T] {

  override def doReset(): Unit = {
    missed.reset()
    found.reset()
  }
}

abstract class ShuffledMasterJoinOperatorFragment[M <: DataModel[M], T <: DataModel[T], J <: DataModel[J]]( // scalastyle:ignore
  val missed: Fragment[T],
  val joined: Fragment[J],
  val joinedDataModel: J)
  extends ShuffledJoinOperatorFragment[M, T]
  with MasterJoin[M, T, J] {

  override def doReset(): Unit = {
    missed.reset()
    joined.reset()
  }
}

abstract class ShuffledMasterJoinUpdateOperatorFragment[M <: DataModel[M], T <: DataModel[T]](
  val missed: Fragment[T],
  val updated: Fragment[T])
  extends ShuffledJoinOperatorFragment[M, T]
  with MasterJoinUpdate[M, T] {

  override def doReset(): Unit = {
    missed.reset()
    updated.reset()
  }
}
