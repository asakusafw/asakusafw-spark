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

import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._
import com.asakusafw.vocabulary.operator.{ MasterBranch => MasterBranchOp }

trait MasterBranch extends JoinOperatorFragmentClassBuilder {

  val opInfo: OperatorInfo
  import opInfo._ // scalastyle:ignore

  override def join(mb: MethodBuilder, masterVar: Var, txVar: Var): Unit = {
    import mb._ // scalastyle:ignore
    block { ctrl =>
      val branch = getOperatorField(mb)
        .invokeV(
          methodDesc.name,
          methodDesc.asType.getReturnType,
          masterVar.push().asType(methodDesc.asType.getArgumentTypes()(0))
            +: txVar.push().asType(methodDesc.asType.getArgumentTypes()(1))
            +: arguments.map { argument =>
              ldc(argument.value)(ClassTag(argument.resolveClass))
            }: _*)
      branch.dup().unlessNotNull {
        branch.pop()
        `throw`(pushNew0(classOf[NullPointerException].asType))
      }
      branchOutputMap.foreach {
        case (output, enum) =>
          branch.dup().unlessNe(
            getStatic(
              methodDesc.asType.getReturnType, enum.name, methodDesc.asType.getReturnType)) {
              getOutputField(mb, output)
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
