/*
 * Copyright 2011-2018 Asakusa Framework Team.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.asakusafw.spark.compiler

import scala.collection.JavaConversions._
import scala.collection.mutable

import org.objectweb.asm.Type

import com.asakusafw.lang.compiler.analyzer.util.OperatorUtil
import com.asakusafw.lang.compiler.model.description.TypeDescription
import com.asakusafw.lang.compiler.model.graph.Operator
import com.asakusafw.lang.compiler.planning.{ Plan, SubPlan }
import com.asakusafw.spark.compiler.graph.JobCompiler
import com.asakusafw.spark.compiler.serializer.{
  BranchKeySerializerClassBuilder,
  BroadcastIdSerializerClassBuilder,
  KryoRegistratorCompiler
}
import com.asakusafw.spark.runtime.{ DefaultClient, JobContext }
import com.asakusafw.spark.runtime.graph.Job
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._

class SparkClientClassBuilder(
  val plan: Plan)(
    implicit context: SparkClientCompiler.Context)
  extends ClassBuilder(
    Type.getType(s"L${GeneratedClassPackageInternalName}/${context.flowId}/SparkClient;"),
    classOf[DefaultClient].asType) {

  override def defMethods(methodDef: MethodDef): Unit = {
    super.defMethods(methodDef)

    methodDef.newMethod(
      "newJob",
      classOf[Job].asType,
      Seq(classOf[JobContext].asType)) { implicit mb =>

        val thisVar :: jobContextVar :: _ = mb.argVars

        val t = JobCompiler.compile(plan)(context.jobCompilerContext)
        val job = pushNew(t)
        job.dup().invokeInit(jobContextVar.push())
        `return`(job)
      }

    val branchKeysType = context.addClass(context.branchKeys)
    val broadcastIdsType = context.addClass(context.broadcastIds)

    val registrator = KryoRegistratorCompiler.compile(
      OperatorUtil.collectDataTypes(
        plan.getElements.toSet[SubPlan].flatMap(_.getOperators.toSet[Operator]))
        .toSet[TypeDescription]
        .map(_.asType),
      context.addClass(new BranchKeySerializerClassBuilder(branchKeysType)),
      context.addClass(new BroadcastIdSerializerClassBuilder(broadcastIdsType)))

    methodDef.newMethod("kryoRegistrator", classOf[String].asType, Seq.empty) { implicit mb =>
      `return`(ldc(registrator.getClassName))
    }
  }
}
