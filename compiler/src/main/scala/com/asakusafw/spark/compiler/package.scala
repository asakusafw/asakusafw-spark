/*
 * Copyright 2011-2021 Asakusa Framework Team.
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
package com.asakusafw.spark

import java.lang.{ Boolean => JBoolean }

import scala.collection.JavaConversions._
import org.objectweb.asm.Type
import com.asakusafw.lang.compiler.analyzer.util.{ BranchOperatorUtil, MasterJoinOperatorUtil }
import com.asakusafw.lang.compiler.api.CompilerOptions
import com.asakusafw.lang.compiler.api.reference.DataModelReference
import com.asakusafw.lang.compiler.model.PropertyName
import com.asakusafw.lang.compiler.model.description._
import com.asakusafw.lang.compiler.model.graph._
import com.asakusafw.lang.compiler.planning.SubPlan
import com.asakusafw.spark.compiler.planning.{ NameInfo, SubPlanInfo }

package object compiler {

  val GeneratedClassPackageInternalName = "com/asakusafw/spark/generated"

  implicit class AugmentedTypeDescription(val desc: TypeDescription) extends AnyVal {

    def asType: Type =
      desc match {
        case desc: BasicTypeDescription => desc.asType
        case desc: ClassDescription => desc.asType
        case desc: ArrayTypeDescription => desc.asType
      }
  }

  implicit class AugmentedReifiableTypeDescription(
    val desc: ReifiableTypeDescription) extends AnyVal {

    def resolveClass(
      implicit provider: ClassLoaderProvider): Class[_] = {
      desc.resolve(provider.classLoader)
    }
  }

  implicit class AugmentedBasicTypeDescription(val desc: BasicTypeDescription) extends AnyVal {

    def asType: Type = {
      import BasicTypeDescription.BasicTypeKind // scalastyle:ignore
      desc.getBasicTypeKind match {
        case BasicTypeKind.VOID => Type.VOID_TYPE
        case BasicTypeKind.INT => Type.INT_TYPE
        case BasicTypeKind.LONG => Type.LONG_TYPE
        case BasicTypeKind.FLOAT => Type.FLOAT_TYPE
        case BasicTypeKind.DOUBLE => Type.DOUBLE_TYPE
        case BasicTypeKind.SHORT => Type.SHORT_TYPE
        case BasicTypeKind.CHAR => Type.CHAR_TYPE
        case BasicTypeKind.BYTE => Type.BYTE_TYPE
        case BasicTypeKind.BOOLEAN => Type.BOOLEAN_TYPE
      }
    }
  }

  implicit class AugmentedClassDescription(val desc: ClassDescription) extends AnyVal {

    def asType: Type = Type.getObjectType(desc.getInternalName)
  }

  implicit class AugmentedArrayTypeDescription(val desc: ArrayTypeDescription) extends AnyVal {

    def asType: Type = Type.getObjectType(s"[${desc.getComponentType.asType.getDescriptor}")
  }

  implicit class AugmentedCompilerOptions(val options: CompilerOptions) extends AnyVal {

    import SparkClientCompiler.Options._ // scalastyle:ignore

    def verifyPlan: Boolean = {
      JBoolean.parseBoolean(options.get(SparkPlanVerify, false.toString))
    }

    def useInputDirect: Boolean = {
      JBoolean.parseBoolean(options.get(SparkInputDirect, true.toString))
    }

    def useOutputDirect: Boolean = {
      JBoolean.parseBoolean(options.get(SparkOutputDirect, true.toString))
    }
  }

  implicit class AugmentedOperator(val operator: Operator) extends AnyVal {

    def annotationDesc: AnnotationDescription = {
      operator.asInstanceOf[UserOperator].getAnnotation
    }

    def implementationClass: ClassDescription = {
      operator.asInstanceOf[UserOperator].getImplementationClass
    }

    def methodDesc: MethodDescription = {
      operator.asInstanceOf[UserOperator].getMethod
    }

    def inputs: Seq[OperatorInput] = {
      operator.getInputs
    }

    def outputs: Seq[OperatorOutput] = {
      operator.getOutputs
    }

    def arguments: Seq[OperatorArgument] = {
      operator.getArguments
    }

    def nonBroadcastInputs: Seq[OperatorInput] = {
      operator.inputs.filter(_.getInputUnit != OperatorInput.InputUnit.WHOLE)
    }

    def branchOutputMap(
      implicit provider: ClassLoaderProvider): Map[OperatorOutput, Enum[_]] = {
      BranchOperatorUtil.getOutputMap(provider.classLoader, operator).toMap
    }

    def selectionMethod(
      implicit provider: ClassLoaderProvider): Option[(String, Type)] = {
      Option(MasterJoinOperatorUtil.getSelection(provider.classLoader, operator))
        .map(method => (method.getName, Type.getType(method)))
    }
  }

  implicit class AugmentedAnnotationDescription(val ad: AnnotationDescription) extends AnyVal {

    def elements: Map[String, ValueDescription] = ad.getElements.toMap

    def resolveClass(
      implicit provider: ClassLoaderProvider): Class[_] = {
      ad.getDeclaringClass.resolveClass
    }
  }

  implicit class AugmentedMethodDescirption(val md: MethodDescription) extends AnyVal {

    def asType(
      implicit provider: ClassLoaderProvider): Type = {
      Type.getType(md.resolve(provider.classLoader))
    }

    def name: String = md.getName

    def parameterClasses(
      implicit provider: ClassLoaderProvider): Seq[Class[_]] = {
      md.getParameterTypes.map(_.resolve(provider.classLoader))
    }
  }

  implicit class AugmentedOperatorPort(val op: OperatorPort) extends AnyVal {

    def dataModelRef(
      implicit provider: DataModelLoaderProvider): DataModelReference = {
      provider.dataModelLoader.load(op.getDataType)
    }

    def dataModelType(
      implicit provider: DataModelLoaderProvider): Type = {
      dataModelRef.getDeclaration.asType
    }

    def dataModelClass(
      implicit dataModelProvider: DataModelLoaderProvider,
      classLoaderProvider: ClassLoaderProvider): Class[_] = {
      dataModelRef.getDeclaration.resolve(classLoaderProvider.classLoader)
    }
  }

  implicit class AugmentedOperatorArgument(val oa: OperatorArgument) extends AnyVal {

    def asType: Type = oa.getValue.getValueType.asType

    def resolveClass(
      implicit provider: ClassLoaderProvider): Class[_] = {
      oa.getValue.getValueType.getErasure.resolve(provider.classLoader)
    }

    def value(
      implicit provider: ClassLoaderProvider): Any = {
      oa.getValue.value
    }
  }

  implicit class AugmentedValueDescription(val vd: ValueDescription) extends AnyVal {

    def value(
      implicit provider: ClassLoaderProvider): Any = {
      vd.resolve(provider.classLoader)
    }
  }

  implicit class AugmentedDataModelReference(val dataModelRef: DataModelReference) extends AnyVal {

    def groupingTypes(propertyNames: Seq[PropertyName]): Seq[Type] = {
      propertyNames.map { propertyName =>
        dataModelRef.findProperty(propertyName).getType.asType
      }
    }

    def orderingTypes(orderings: Seq[Group.Ordering]): Seq[(Type, Boolean)] = {
      orderings.map { ordering =>
        (dataModelRef.findProperty(ordering.getPropertyName).getType.asType,
          ordering.getDirection == Group.Direction.ASCENDANT)
      }
    }
  }

  implicit class SubPlanNames(val subplan: SubPlan) extends AnyVal {

    def name: String = {
      Option(subplan.getAttribute(classOf[NameInfo]))
        .map(_.getName)
        .getOrElse("N/A")
    }

    def label: String = {
      Seq(
        Option(subplan.getAttribute(classOf[NameInfo]))
          .map(_.getName),
        Option(subplan.getAttribute(classOf[SubPlanInfo]))
          .flatMap(info => Option(info.getLabel)))
        .flatten match {
          case Seq() => "N/A"
          case s: Seq[String] => s.mkString(":")
        }
    }
  }
}
