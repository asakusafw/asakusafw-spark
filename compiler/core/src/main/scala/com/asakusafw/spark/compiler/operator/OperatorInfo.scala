package com.asakusafw.spark.compiler
package operator

import scala.collection.JavaConversions._

import org.objectweb.asm.Type

import com.asakusafw.lang.compiler.analyzer.util.{ BranchOperatorUtil, MasterJoinOperatorUtil }
import com.asakusafw.lang.compiler.api.JobflowProcessor.{ Context => JPContext }
import com.asakusafw.lang.compiler.api.reference.DataModelReference
import com.asakusafw.lang.compiler.model.description._
import com.asakusafw.lang.compiler.model.graph._
import com.asakusafw.spark.tools.asm._

class OperatorInfo(operator: Operator)(implicit jpContext: JPContext) {

  lazy val annotationDesc = operator match {
    case op: UserOperator => op.getAnnotation
  }

  implicit class AugmentedAnnotationDescription(ad: AnnotationDescription) {

    def resolveClass: Class[_] = ad.getDeclaringClass.resolve(jpContext.getClassLoader)
  }

  lazy val implementationClassType = operator match {
    case op: UserOperator => op.getImplementationClass.asType
  }

  lazy val methodDesc = operator match {
    case op: UserOperator => op.getMethod
  }

  implicit class AugmentedMethodDescirption(md: MethodDescription) {

    def asType: Type = Type.getType(md.resolve(jpContext.getClassLoader))

    def name: String = methodDesc.getName

    def parameterClasses: Seq[Class[_]] = methodDesc.getParameterTypes.map(_.resolve(jpContext.getClassLoader))
  }

  lazy val inputs = operator.getInputs.toSeq

  lazy val outputs = operator.getOutputs.toSeq

  implicit class AugmentedOperatorInput(oi: OperatorPort) {

    def dataModelRef: DataModelReference = jpContext.getDataModelLoader.load(oi.getDataType)

    def dataModelType: Type = dataModelRef.getDeclaration.asType

    def dataModelClass: Class[_] = dataModelRef.getDeclaration.resolve(jpContext.getClassLoader)
  }

  lazy val arguments = operator.getArguments.toSeq

  implicit class AugmentedOperatorArgument(oa: OperatorArgument) {

    def asType: Type = oa.getValue.getValueType.asType

    def resolveClass: Class[_] = oa.getValue.getValueType.resolve(jpContext.getClassLoader)

    def value: Any = oa.getValue.resolve(jpContext.getClassLoader)
  }

  lazy val branchOutputMap = BranchOperatorUtil.getOutputMap(jpContext.getClassLoader, operator).toMap

  lazy val selectionMethod =
    Option(MasterJoinOperatorUtil.getSelection(jpContext.getClassLoader, operator))
      .map(method => (method.getName, Type.getType(method)))
}
