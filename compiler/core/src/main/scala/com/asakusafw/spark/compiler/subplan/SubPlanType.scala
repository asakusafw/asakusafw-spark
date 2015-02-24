package com.asakusafw.spark.compiler.subplan

// TODO should be replaced by subplan attribute.
trait SubPlanType

object SubPlanType {

  case object CoGroupSubPlan extends SubPlanType
}
