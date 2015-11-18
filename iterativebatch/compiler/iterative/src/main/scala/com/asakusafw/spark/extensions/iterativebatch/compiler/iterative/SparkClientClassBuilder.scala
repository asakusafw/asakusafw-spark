/*
 * Copyright 2011-2015 Asakusa Framework Team.
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
package com.asakusafw.spark.extensions.iterativebatch.compiler
package iterative

import scala.collection.JavaConversions._
import scala.concurrent.ExecutionContext

import org.apache.spark.SparkContext
import org.objectweb.asm.Type

import com.asakusafw.lang.compiler.analyzer.util.OperatorUtil
import com.asakusafw.lang.compiler.model.description.TypeDescription
import com.asakusafw.lang.compiler.model.graph.Operator
import com.asakusafw.lang.compiler.planning.{ Plan, SubPlan }
import com.asakusafw.spark.compiler.`package`._
import com.asakusafw.spark.compiler.serializer.{
  BranchKeySerializerClassBuilder,
  BroadcastIdSerializerClassBuilder,
  KryoRegistratorCompiler
}
import com.asakusafw.spark.tools.asm._

import com.asakusafw.spark.extensions.iterativebatch.runtime.IterativeBatchExecutor
import com.asakusafw.spark.extensions.iterativebatch.runtime.iterative.SparkClient

class SparkClientClassBuilder(
  plan: Plan)(
    implicit context: SparkClientCompiler.Context)
  extends ClassBuilder(
    Type.getType(s"L${GeneratedClassPackageInternalName}/${context.flowId}/SparkClient;"),
    classOf[SparkClient].asType) {

  override def defFields(fieldDef: FieldDef): Unit = {
    fieldDef.newField("executor", classOf[IterativeBatchExecutor].asType)
  }

  override def defMethods(methodDef: MethodDef): Unit = {
    super.defMethods(methodDef)

    methodDef.newMethod(
      "newIterativeBatchExecutor",
      classOf[IterativeBatchExecutor].asType,
      Seq(Type.INT_TYPE, classOf[ExecutionContext].asType, classOf[SparkContext].asType)) { mb =>
        import mb._ // scalastyle:ignore

        val numSlotsVar = `var`(Type.INT_TYPE, thisVar.nextLocal)
        val ecVar = `var`(classOf[ExecutionContext].asType, numSlotsVar.nextLocal)
        val scVar = `var`(classOf[SparkContext].asType, ecVar.nextLocal)

        val t = IterativeBatchExecutorCompiler.compile(plan)(
          context.iterativeBatchExecutorCompilerContext)
        val executor = pushNew(t)
        executor.dup().invokeInit(numSlotsVar.push(), ecVar.push(), scVar.push())
        `return`(executor)
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

    methodDef.newMethod("kryoRegistrator", classOf[String].asType, Seq.empty) { mb =>
      import mb._ // scalastyle:ignore
      `return`(ldc(registrator.getClassName))
    }
  }
}
