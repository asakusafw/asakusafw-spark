package com.asakusafw.spark.compiler.subplan

// TODO should be replaced by subplan attribute.
trait SubPlanType

object SubPlanType {

  case object InputSubPlan extends SubPlanType
  case object OutputSubPlan extends SubPlanType
  case object MapSubPlan extends SubPlanType
  case object CoGroupSubPlan extends SubPlanType
}
