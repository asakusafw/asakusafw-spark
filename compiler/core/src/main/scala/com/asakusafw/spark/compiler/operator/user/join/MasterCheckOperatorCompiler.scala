package com.asakusafw.spark.compiler
package operator
package user
package join

import java.util.{ List => JList }

import scala.collection.JavaConversions
import scala.collection.JavaConversions._
import scala.reflect.ClassTag

import org.objectweb.asm.Type
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.lang.compiler.analyzer.util.MasterJoinOperatorUtil
import com.asakusafw.lang.compiler.model.graph.{ OperatorOutput, UserOperator }
import com.asakusafw.runtime.core.Result
import com.asakusafw.spark.compiler.operator.FragmentClassBuilder
import com.asakusafw.spark.compiler.spi.UserOperatorCompiler
import com.asakusafw.spark.runtime.fragment.Fragment
import com.asakusafw.spark.runtime.operator.DefaultMasterSelection
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._
import com.asakusafw.vocabulary.operator.MasterCheck

class MasterCheckOperatorCompiler extends UserOperatorCompiler {

  override def of: Class[_] = classOf[MasterCheck]

  override def compile(operator: UserOperator)(implicit context: Context): Type = {
    val annotationDesc = operator.getAnnotation
    assert(annotationDesc.getDeclaringClass.resolve(context.jpContext.getClassLoader) == of)
    val implementationClassType = operator.getImplementationClass.asType

    val selectionMethod =
      Option(MasterJoinOperatorUtil.getSelection(context.jpContext.getClassLoader, operator))
        .map(method => (method.getName, Type.getType(method)))

    val inputs = operator.getInputs.toSeq
    assert(inputs.size >= 2)
    val inputDataModelRefs = inputs.map(input => context.jpContext.getDataModelLoader.load(input.getDataType))
    val inputDataModelTypes = inputDataModelRefs.map(_.getDeclaration.asType)

    val masterDataModelType = inputDataModelTypes(MasterCheck.ID_INPUT_MASTER)
    val txDataModelType = inputDataModelTypes(MasterCheck.ID_INPUT_TRANSACTION)

    val outputs = operator.getOutputs.toSeq
    assert(outputs.size == 2)
    val outputDataModelRefs = outputs.map(output => context.jpContext.getDataModelLoader.load(output.getDataType))
    val outputDataModelTypes = outputDataModelRefs.map(_.getDeclaration.asType)

    outputDataModelTypes.foreach(t => assert(t == txDataModelType))

    val foundOutput = outputs(MasterCheck.ID_OUTPUT_FOUND)
    val missedOutput = outputs(MasterCheck.ID_OUTPUT_MISSED)

    val arguments = operator.getArguments.toSeq

    val builder = new JoinOperatorClassBuilder(context.flowId) {

      override def operatorType: Type = implementationClassType
      override def operatorOutputs: Seq[OperatorOutput] = outputs

      override def masterType: Type = masterDataModelType
      override def txType: Type = txDataModelType
      override def masterSelection: Option[(String, Type)] = selectionMethod

      override def join(mb: MethodBuilder, ctrl: LoopControl, masterVar: Var, txVar: Var): Unit = {
        import mb._
        masterVar.push().ifNull({
          getOutputField(mb, missedOutput)
        }, {
          getOutputField(mb, foundOutput)
        }).invokeV("add", txVar.push().asType(classOf[AnyRef].asType))
      }
    }

    context.jpContext.addClass(builder)
  }
}
