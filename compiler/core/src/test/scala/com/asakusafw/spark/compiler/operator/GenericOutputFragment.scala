package com.asakusafw.spark.compiler.operator

import scala.reflect.{ classTag, ClassTag }

import com.asakusafw.spark.runtime.fragment.OutputFragment
import com.asakusafw.runtime.model.DataModel

class GenericOutputFragment[T <: DataModel[T]: ClassTag] extends OutputFragment[T] {

  override def newDataModel(): T = {
    classTag[T].runtimeClass.newInstance().asInstanceOf[T]
  }
}
