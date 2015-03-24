package com.asakusafw.spark.compiler
package operator

import scala.collection.JavaConversions._

import org.objectweb.asm.Type

import com.asakusafw.lang.compiler.analyzer.util.{ BranchOperatorUtil, MasterJoinOperatorUtil }
import com.asakusafw.lang.compiler.api.JobflowProcessor.{ Context => JPContext }
import com.asakusafw.lang.compiler.api.reference.DataModelReference
import com.asakusafw.lang.compiler.model.graph._

class OperatorInfo(operator: Operator)(implicit jpContext: JPContext) {

  lazy val annotationDesc = operator match {
    case op: UserOperator => op.getAnnotation
  }

  lazy val implementationClassType = operator match {
    case op: UserOperator => op.getImplementationClass.asType
  }

  lazy val methodDesc = operator match {
    case op: UserOperator => op.getMethod
  }

  lazy val methodType = Type.getType(methodDesc.resolve(jpContext.getClassLoader))

  lazy val inputs = operator.getInputs.toSeq

  lazy val inputDataModelRefs = inputs.map(input => jpContext.getDataModelLoader.load(input.getDataType))
  def inputDataModelRefs(input: OperatorInput): DataModelReference = inputDataModelRefs(inputs.indexOf(input))

  lazy val inputDataModelTypes = inputDataModelRefs.map(_.getDeclaration.asType)
  def inputDataModelTypes(input: OperatorInput): Type = inputDataModelTypes(inputs.indexOf(input))

  lazy val outputs = operator.getOutputs.toSeq

  lazy val outputDataModelRefs = outputs.map(output => jpContext.getDataModelLoader.load(output.getDataType))
  def outputDataModelRefs(output: OperatorOutput): DataModelReference = outputDataModelRefs(outputs.indexOf(output))

  lazy val outputDataModelTypes = outputDataModelRefs.map(_.getDeclaration.asType)
  def outputDataModelTypes(output: OperatorOutput): Type = outputDataModelTypes(outputs.indexOf(output))

  lazy val arguments = operator.getArguments.toSeq

  lazy val argumentTypes = arguments.map(_.getValue.getValueType.asType)

  lazy val branchOutputMap = BranchOperatorUtil.getOutputMap(jpContext.getClassLoader, operator).toMap

  lazy val selectionMethod =
    Option(MasterJoinOperatorUtil.getSelection(jpContext.getClassLoader, operator))
      .map(method => (method.getName, Type.getType(method)))
}
