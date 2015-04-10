package com.asakusafw.spark.compiler
package operator
package user
package join

import scala.reflect.ClassTag

import org.objectweb.asm.Type

import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._
import com.asakusafw.vocabulary.operator.{ MasterCheck => MasterCheckOp }

trait MasterCheck extends JoinOperatorFragmentClassBuilder {

  val opInfo: OperatorInfo
  import opInfo._

  override def join(mb: MethodBuilder, masterVar: Var, txVar: Var): Unit = {
    import mb._
    masterVar.push().ifNull({
      getOutputField(mb, outputs(MasterCheckOp.ID_OUTPUT_MISSED))
    }, {
      getOutputField(mb, outputs(MasterCheckOp.ID_OUTPUT_FOUND))
    }).invokeV("add", txVar.push().asType(classOf[AnyRef].asType))
  }
}
