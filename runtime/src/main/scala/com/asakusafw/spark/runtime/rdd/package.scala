package com.asakusafw.spark.runtime

import scala.language.implicitConversions
import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD

package object rdd {

  implicit def rddToBranchRDDFunctions[T: ClassTag](rdd: RDD[T]): BranchRDDFunctions[T] = {
    new BranchRDDFunctions(rdd)
  }
}
