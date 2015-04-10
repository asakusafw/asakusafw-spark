package com.asakusafw.spark.compiler
package operator
package user
package join

import scala.reflect.ClassTag

import org.objectweb.asm.Type

import com.asakusafw.lang.compiler.analyzer.util.PropertyMapping
import com.asakusafw.spark.runtime.util.ValueOptionOps
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._
import com.asakusafw.vocabulary.operator.{ MasterJoin => MasterJoinOp }

trait MasterJoin extends JoinOperatorFragmentClassBuilder {

  val opInfo: OperatorInfo
  import opInfo._

  val mappings: Seq[PropertyMapping]

  override def defFields(fieldDef: FieldDef): Unit = {
    super.defFields(fieldDef)
    fieldDef.newField("joinedDataModel", outputs(MasterJoinOp.ID_OUTPUT_JOINED).dataModelType)
  }

  override def initFields(mb: MethodBuilder): Unit = {
    import mb._
    thisVar.push().putField(
      "joinedDataModel",
      outputs(MasterJoinOp.ID_OUTPUT_JOINED).dataModelType,
      pushNew0(outputs(MasterJoinOp.ID_OUTPUT_JOINED).dataModelType))
  }

  override def join(mb: MethodBuilder, masterVar: Var, txVar: Var): Unit = {
    import mb._
    block { ctrl =>
      masterVar.push().unlessNotNull {
        getOutputField(mb, outputs(MasterJoinOp.ID_OUTPUT_MISSED))
          .invokeV("add", txVar.push().asType(classOf[AnyRef].asType))
        ctrl.break()
      }

      val vars = Seq(masterVar, txVar)

      thisVar.push().getField("joinedDataModel", outputs(MasterJoinOp.ID_OUTPUT_JOINED).dataModelType).invokeV("reset")

      mappings.foreach { mapping =>
        val src = inputs.indexOf(mapping.getSourcePort)
        val srcVar = vars(src)
        val srcProperty = inputs(src).dataModelRef.findProperty(mapping.getSourceProperty)
        val destProperty = outputs(MasterJoinOp.ID_OUTPUT_JOINED).dataModelRef.findProperty(mapping.getDestinationProperty)
        assert(srcProperty.getType.asType == destProperty.getType.asType,
          s"The source and destination types should be the same: (${srcProperty.getType}, ${destProperty.getType}")

        getStatic(ValueOptionOps.getClass.asType, "MODULE$", ValueOptionOps.getClass.asType)
          .invokeV(
            "copy",
            srcVar.push()
              .invokeV(srcProperty.getDeclaration.getName, srcProperty.getType.asType),
            thisVar.push().getField("joinedDataModel", outputs(MasterJoinOp.ID_OUTPUT_JOINED).dataModelType)
              .invokeV(destProperty.getDeclaration.getName, destProperty.getType.asType))
      }

      getOutputField(mb, outputs(MasterJoinOp.ID_OUTPUT_JOINED))
        .invokeV("add",
          thisVar.push().getField("joinedDataModel", outputs(MasterJoinOp.ID_OUTPUT_JOINED).dataModelType)
            .asType(classOf[AnyRef].asType))
    }
  }
}
