package org.apache.spark

package object backdoor {

  implicit class SparkContextBackdoor(val sc: SparkContext) extends AnyVal {

    def cleanF[F <: AnyRef](f: F, checkSerializable: Boolean = true): F = {
      sc.clean(f, checkSerializable)
    }
  }
}
