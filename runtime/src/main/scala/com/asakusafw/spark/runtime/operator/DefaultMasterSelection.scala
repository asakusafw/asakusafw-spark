package com.asakusafw.spark.runtime.operator

import java.util.{ List => JList }

import scala.collection.JavaConversions._

object DefaultMasterSelection {

  def select[M, T](masters: JList[M], tx: T): M = {
    masters.headOption.getOrElse(null.asInstanceOf[M])
  }
}
