package com.asakusafw.spark.compiler

import java.io.File

import scala.collection.mutable
import scala.collection.JavaConversions._

import com.asakusafw.lang.compiler.api.CompilerOptions
import com.asakusafw.lang.compiler.api.JobflowProcessor.{ Context => JPContext }
import com.asakusafw.lang.compiler.api.testing.MockJobflowProcessorContext
import com.asakusafw.spark.compiler.subplan.{
  BranchKeysClassBuilder,
  BroadcastIdsClassBuilder
}

trait CompilerContext {

  def newContext(
    flowId: String,
    outputDir: File): SparkClientCompiler.Context = {
    newContext(
      flowId,
      new MockJobflowProcessorContext(
        new CompilerOptions("buildid", "", Map.empty[String, String]),
        Thread.currentThread.getContextClassLoader,
        outputDir))
  }

  def newContext(
    flowId: String,
    jpContext: JPContext): SparkClientCompiler.Context = {
    SparkClientCompiler.Context(
      flowId = flowId,
      jpContext = jpContext,
      externalInputs = mutable.Map.empty,
      branchKeys = new BranchKeysClassBuilder(flowId),
      broadcastIds = new BroadcastIdsClassBuilder(flowId))
  }
}
