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
import com.asakusafw.vocabulary.operator.{ MasterJoinUpdate => MasterJoinUpdateOp }

trait MasterJoinUpdate extends JoinOperatorFragmentClassBuilder {

  val operatorInfo: OperatorInfo
  import operatorInfo._ // scalastyle:ignore

  override def join(mb: MethodBuilder, masterVar: Var, txVar: Var): Unit = {
    import mb._ // scalastyle:ignore
    masterVar.push().ifNull({
      getOutputField(mb, outputs(MasterJoinUpdateOp.ID_OUTPUT_MISSED))
    }, {
      getOperatorField(mb)
        .invokeV(
          methodDesc.getName,
          masterVar.push().asType(methodDesc.asType.getArgumentTypes()(0))
            +: txVar.push().asType(methodDesc.asType.getArgumentTypes()(1))
            +: arguments.map { argument =>
              ldc(argument.value)(ClassTag(argument.resolveClass))
            }: _*)
      getOutputField(mb, outputs(MasterJoinUpdateOp.ID_OUTPUT_UPDATED))
    }).invokeV("add", txVar.push().asType(classOf[AnyRef].asType))
  }
}
