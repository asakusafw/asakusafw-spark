package org.apache.spark

import org.apache.spark.util.backdoor.CallSite

package object backdoor {

  type HttpServer = org.apache.spark.HttpServer
  type SecurityManager = org.apache.spark.SecurityManager

  implicit class SparkContextBackdoor(val sc: SparkContext) extends AnyVal {

    def clean[F <: AnyRef](f: F, checkSerializable: Boolean = true): F = {
      sc.clean(f, checkSerializable)
    }

    def setCallSite(callSite: CallSite) = sc.setCallSite(callSite)
  }
}
