package com.asakusafw.spark.tools.graph.plan

import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner

import scala.collection.JavaConversions._

import com.asakusafw.lang.compiler.model.graph.MockOperators
import com.asakusafw.lang.compiler.planning._
import com.asakusafw.utils.graph.Graphs

@RunWith(classOf[JUnitRunner])
class AugmentedPlanSpecTest extends AugmentedPlanSpec

class AugmentedPlanSpec extends FlatSpec {

  behavior of "AugmentedPlan"

  it should "convert a plan to dependency graph of subplans" in {
    val ops = new MockOperators()
      .marker("m0", PlanMarker.BEGIN)
      .operator("a")
      .marker("m1", PlanMarker.CHECKPOINT)
      .operator("b")
      .marker("m2", PlanMarker.END)
      .connect("m0", "a")
      .connect("a", "m1")
      .connect("m1", "b")
      .connect("b", "m2")
    val plan = PlanBuilder.from(ops.getAsSet("a"))
      .add(ops.getMarkers("m0"), ops.getMarkers("m1"))
      .add(ops.getMarkers("m1"), ops.getMarkers("m2")).build().getPlan
    assert(plan.getElements.size === 2)

    val subplans = Graphs.sortPostOrder(plan.toDependencyGraph)
    assert(subplans.size === 2)
    assert(subplans(0).getInputs.head.getOperator.getOriginalSerialNumber
      === ops.getMarkers("m0").head.getOriginalSerialNumber)
    assert(subplans(1).getInputs.head.getOperator.getOriginalSerialNumber
      === ops.getMarkers("m1").head.getOriginalSerialNumber)
  }
}
