package com.asakusafw.spark.tools.graph

import scala.collection.JavaConversions._

import com.asakusafw.lang.compiler.planning.{ Plan, SubPlan }
import com.asakusafw.utils.graph.{ Graph, Graphs }

package object plan {

  implicit class AugmentedPlan(val plan: Plan) extends AnyVal {

    def toDependencyGraph: Graph[SubPlan] = {
      val graph = Graphs.newInstance[SubPlan]
      plan.getElements.foreach { subplan =>
        graph.addNode(subplan)
        subplan.getInputs.foreach { input =>
          input.getOpposites.foreach { opposite =>
            graph.addEdge(subplan, opposite.getOwner)
          }
        }
        subplan.getOutputs.foreach { output =>
          output.getOpposites.foreach { opposite =>
            graph.addEdge(opposite.getOwner, subplan)
          }
        }
      }
      graph
    }
  }
}
