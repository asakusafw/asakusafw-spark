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
package com.asakusafw.spark.compiler
package operator
package user
package join

import scala.reflect.ClassTag

import org.objectweb.asm.Type

import com.asakusafw.lang.compiler.model.graph.UserOperator
import com.asakusafw.spark.compiler.spi.OperatorCompiler
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._
import com.asakusafw.vocabulary.operator.{ MasterBranch => MasterBranchOp }

trait MasterBranch extends JoinOperatorFragmentClassBuilder {

  implicit def context: OperatorCompiler.Context

  def operator: UserOperator

  override def join(masterVar: Var, txVar: Var)(implicit mb: MethodBuilder): Unit = {
    block { ctrl =>
      val branch = getOperatorField()
        .invokeV(
          operator.methodDesc.name,
          operator.methodDesc.asType.getReturnType,
          masterVar.push().asType(operator.methodDesc.asType.getArgumentTypes()(0))
            +: txVar.push().asType(operator.methodDesc.asType.getArgumentTypes()(1))
            +: operator.arguments.map { argument =>
              Option(argument.value).map { value =>
                ldc(value)(ClassTag(argument.resolveClass), implicitly)
              }.getOrElse {
                pushNull(argument.resolveClass.asType)
              }
            }: _*)
      branch.dup().unlessNotNull {
        branch.pop()
        `throw`(pushNew0(classOf[NullPointerException].asType))
      }
      operator.branchOutputMap.foreach {
        case (output, enum) =>
          branch.dup().unlessNe(
            getStatic(
              operator.methodDesc.asType.getReturnType,
              enum.name,
              operator.methodDesc.asType.getReturnType)) {
              getOutputField(output)
                .invokeV("add", txVar.push().asType(classOf[AnyRef].asType))
              branch.pop()
              ctrl.break()
            }
      }
      branch.pop()
      `throw`(pushNew0(classOf[AssertionError].asType))
    }
  }
}
