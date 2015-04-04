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
  import opInfo._

  override def join(mb: MethodBuilder, ctrl: LoopControl, masterVar: Var, txVar: Var): Unit = {
    import mb._
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
          getStatic(methodDesc.asType.getReturnType, enum.name, methodDesc.asType.getReturnType)) {
            getOutputField(mb, output)
              .invokeV("add", txVar.push().asType(classOf[AnyRef].asType))
            branch.pop()
            ctrl.continue()
          }
    }
    branch.pop()
    `throw`(pushNew0(classOf[AssertionError].asType))
  }
}
