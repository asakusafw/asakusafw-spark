package org.apache.spark

package object backdoor {

  type HttpServer = org.apache.spark.HttpServer
  type SecurityManager = org.apache.spark.SecurityManager

  implicit class SparkContextBackdoor(val sc: SparkContext) extends AnyVal {

    def cleanF[F <: AnyRef](f: F, checkSerializable: Boolean = true): F = {
      sc.clean(f, checkSerializable)
    }
  }
}
