package com.asakusafw.spark.compiler
package operator
package user
package join

import scala.collection.JavaConversions._

import org.objectweb.asm.Type
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.lang.compiler.analyzer.util.JoinedModelUtil
import com.asakusafw.lang.compiler.model.graph.UserOperator
import com.asakusafw.spark.compiler.spi.OperatorType
import com.asakusafw.spark.runtime.fragment.Fragment
import com.asakusafw.spark.runtime.util.ValueOptionOps
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._
import com.asakusafw.vocabulary.operator.MasterJoin

class MasterJoinOperatorCompiler extends UserOperatorCompiler {

  override def support(operator: UserOperator)(implicit context: Context): Boolean = {
    val operatorInfo = new OperatorInfo(operator)(context.jpContext)
    import operatorInfo._
    annotationDesc.resolveClass == classOf[MasterJoin]
  }

  override def operatorType: OperatorType = OperatorType.CoGroupType

  override def compile(operator: UserOperator)(implicit context: Context): Type = {
    assert(support(operator))

    val operatorInfo = new OperatorInfo(operator)(context.jpContext)
    import operatorInfo._

    assert(inputs.size >= 2)
    assert(outputs.size == 2)

    assert(outputs(MasterJoin.ID_OUTPUT_MISSED).dataModelType
      == inputs(MasterJoin.ID_INPUT_TRANSACTION).dataModelType)

    val mappings = JoinedModelUtil.getPropertyMappings(context.jpContext.getClassLoader, operator).toSeq

    val builder = new JoinOperatorFragmentClassBuilder(
      context.flowId,
      implementationClassType,
      outputs,
      inputs(MasterJoin.ID_INPUT_MASTER).dataModelType,
      inputs(MasterJoin.ID_INPUT_TRANSACTION).dataModelType,
      selectionMethod) {

      override def defFields(fieldDef: FieldDef): Unit = {
        super.defFields(fieldDef)
        fieldDef.newField("joinedDataModel", outputs(MasterJoin.ID_OUTPUT_JOINED).dataModelType)
      }

      override def initFields(mb: MethodBuilder): Unit = {
        import mb._
        thisVar.push().putField(
          "joinedDataModel",
          outputs(MasterJoin.ID_OUTPUT_JOINED).dataModelType,
          pushNew0(outputs(MasterJoin.ID_OUTPUT_JOINED).dataModelType))
      }

      override def join(mb: MethodBuilder, ctrl: LoopControl, masterVar: Var, txVar: Var): Unit = {
        import mb._
        masterVar.push().unlessNotNull {
          getOutputField(mb, outputs(MasterJoin.ID_OUTPUT_MISSED))
            .invokeV("add", txVar.push().asType(classOf[AnyRef].asType))
          ctrl.continue()
        }

        val vars = Seq(masterVar, txVar)

        thisVar.push().getField("joinedDataModel", outputs(MasterJoin.ID_OUTPUT_JOINED).dataModelType).invokeV("reset")

        mappings.foreach { mapping =>
          val src = inputs.indexOf(mapping.getSourcePort)
          val srcVar = vars(src)
          val srcProperty = inputs(src).dataModelRef.findProperty(mapping.getSourceProperty)
          val destProperty = outputs(MasterJoin.ID_OUTPUT_JOINED).dataModelRef.findProperty(mapping.getDestinationProperty)
          assert(srcProperty.getType.asType == destProperty.getType.asType)

          getStatic(ValueOptionOps.getClass.asType, "MODULE$", ValueOptionOps.getClass.asType)
            .invokeV(
              "copy",
              srcVar.push()
                .invokeV(srcProperty.getDeclaration.getName, srcProperty.getType.asType),
              thisVar.push().getField("joinedDataModel", outputs(MasterJoin.ID_OUTPUT_JOINED).dataModelType)
                .invokeV(destProperty.getDeclaration.getName, destProperty.getType.asType))
        }

        getOutputField(mb, outputs(MasterJoin.ID_OUTPUT_JOINED))
          .invokeV("add",
            thisVar.push().getField("joinedDataModel", outputs(MasterJoin.ID_OUTPUT_JOINED).dataModelType)
              .asType(classOf[AnyRef].asType))
      }
    }

    context.jpContext.addClass(builder)
  }
}
