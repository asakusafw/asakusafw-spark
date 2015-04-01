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

  val opInfo: OperatorInfo
  import opInfo._

  override def join(mb: MethodBuilder, ctrl: LoopControl, masterVar: Var, txVar: Var): Unit = {
    import mb._
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
