package com.asakusafw.spark.compiler
package operator
package user
package join

import scala.collection.JavaConversions._

import org.objectweb.asm.Type
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.lang.compiler.analyzer.util.JoinedModelUtil
import com.asakusafw.lang.compiler.model.graph.UserOperator
import com.asakusafw.spark.compiler.spi.UserOperatorCompiler
import com.asakusafw.spark.runtime.fragment.Fragment
import com.asakusafw.spark.runtime.util.ValueOptionOps
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._
import com.asakusafw.vocabulary.operator.MasterJoin

class MasterJoinOperatorCompiler extends UserOperatorCompiler {

  override def support(operator: UserOperator)(implicit context: Context): Boolean = {
    val operatorInfo = new OperatorInfo(operator)(context.jpContext)
    operatorInfo.annotationClass == classOf[MasterJoin]
  }

  override def operatorType: OperatorType = OperatorType.CoGroupType

  override def compile(operator: UserOperator)(implicit context: Context): Type = {
    assert(support(operator))

    val operatorInfo = new OperatorInfo(operator)(context.jpContext)

    assert(operatorInfo.inputs.size >= 2)
    assert(operatorInfo.outputs.size == 2)

    assert(operatorInfo.outputDataModelTypes(MasterJoin.ID_OUTPUT_MISSED)
      == operatorInfo.inputDataModelTypes(MasterJoin.ID_INPUT_TRANSACTION))

    val mappings = JoinedModelUtil.getPropertyMappings(context.jpContext.getClassLoader, operator).toSeq

    val builder = new JoinOperatorFragmentClassBuilder(
      context.flowId,
      operatorInfo.implementationClassType,
      operatorInfo.outputs,
      operatorInfo.inputDataModelTypes(MasterJoin.ID_INPUT_MASTER),
      operatorInfo.inputDataModelTypes(MasterJoin.ID_INPUT_TRANSACTION),
      operatorInfo.selectionMethod) {

      override def defFields(fieldDef: FieldDef): Unit = {
        super.defFields(fieldDef)
        fieldDef.newField("joinedDataModel",
          operatorInfo.outputDataModelTypes(MasterJoin.ID_OUTPUT_JOINED))
      }

      override def initFields(mb: MethodBuilder): Unit = {
        import mb._
        thisVar.push().putField(
          "joinedDataModel",
          operatorInfo.outputDataModelTypes(MasterJoin.ID_OUTPUT_JOINED),
          pushNew0(operatorInfo.outputDataModelTypes(MasterJoin.ID_OUTPUT_JOINED)))
      }

      override def join(mb: MethodBuilder, ctrl: LoopControl, masterVar: Var, txVar: Var): Unit = {
        import mb._
        masterVar.push().unlessNotNull {
          getOutputField(mb, operatorInfo.outputs(MasterJoin.ID_OUTPUT_MISSED))
            .invokeV("add", txVar.push().asType(classOf[AnyRef].asType))
          ctrl.continue()
        }

        val vars = Seq(masterVar, txVar)

        thisVar.push().getField("joinedDataModel", operatorInfo.outputDataModelTypes(MasterJoin.ID_OUTPUT_JOINED)).invokeV("reset")

        mappings.foreach { mapping =>
          val src = operatorInfo.inputs.indexOf(mapping.getSourcePort)
          val srcVar = vars(src)
          val srcProperty = operatorInfo.inputDataModelRefs(src).findProperty(mapping.getSourceProperty)
          val destProperty = operatorInfo.outputDataModelRefs(MasterJoin.ID_OUTPUT_JOINED).findProperty(mapping.getDestinationProperty)
          assert(srcProperty.getType.asType == destProperty.getType.asType)

          getStatic(ValueOptionOps.getClass.asType, "MODULE$", ValueOptionOps.getClass.asType)
            .invokeV(
              "copy",
              srcVar.push()
                .invokeV(srcProperty.getDeclaration.getName, srcProperty.getType.asType),
              thisVar.push().getField("joinedDataModel", operatorInfo.outputDataModelTypes(MasterJoin.ID_OUTPUT_JOINED))
                .invokeV(destProperty.getDeclaration.getName, destProperty.getType.asType))
        }

        getOutputField(mb, operatorInfo.outputs(MasterJoin.ID_OUTPUT_JOINED))
          .invokeV("add",
            thisVar.push().getField("joinedDataModel", operatorInfo.outputDataModelTypes(MasterJoin.ID_OUTPUT_JOINED))
              .asType(classOf[AnyRef].asType))
      }
    }

    context.jpContext.addClass(builder)
  }
}
