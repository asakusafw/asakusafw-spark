/*
 * Copyright 2011-2015 Asakusa Framework Team.
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
import com.asakusafw.lang.compiler.api.JobflowProcessor.{ Context => JPContext }
import com.asakusafw.lang.compiler.api.reference.DataModelReference
import com.asakusafw.lang.compiler.model.PropertyName
import com.asakusafw.lang.compiler.model.description._
import com.asakusafw.lang.compiler.model.graph._
import com.asakusafw.runtime.value._
import com.asakusafw.spark.tools.asm._

import resource._

package object compiler {

  val GeneratedClassPackageInternalName = "com/asakusafw/generated/spark"

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
      implicit context: SparkClientCompiler.Context): Class[_] = {
      desc.resolve(context.jpContext.getClassLoader)
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

  implicit class AugmentedJobflowProcessorContext(val context: JPContext) extends AnyVal {

    def addClass(builder: ClassBuilder): Type = {
      addClass(builder.thisType, builder.build())
    }

    def addClass(t: Type, bytes: Array[Byte]): Type = {
      for {
        os <- managed(context.addClassFile(new ClassDescription(t.getClassName)))
      } {
        os.write(bytes)
      }
      t
    }
  }

  implicit class AugmentedCompilerOptions(val options: CompilerOptions) extends AnyVal {

    import SparkClientCompiler.Options._ // scalastyle:ignore

    def verifyPlan: Boolean = {
      JBoolean.parseBoolean(options.get(SparkPlanVerify, false.toString))
    }

    def useInputDirect: Boolean = {
      JBoolean.parseBoolean(options.get(SparkInputDirect, true.toString))
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

    def branchOutputMap(
      implicit context: SparkClientCompiler.Context): Map[OperatorOutput, Enum[_]] = {
      BranchOperatorUtil.getOutputMap(context.jpContext.getClassLoader, operator).toMap
    }

    def selectionMethod(
      implicit context: SparkClientCompiler.Context): Option[(String, Type)] = {
      Option(MasterJoinOperatorUtil.getSelection(context.jpContext.getClassLoader, operator))
        .map(method => (method.getName, Type.getType(method)))
    }
  }

  implicit class AugmentedAnnotationDescription(val ad: AnnotationDescription) extends AnyVal {

    def elements: Map[String, ValueDescription] = ad.getElements.toMap

    def resolveClass(
      implicit context: SparkClientCompiler.Context): Class[_] = {
      ad.getDeclaringClass.resolveClass
    }
  }

  implicit class AugmentedMethodDescirption(val md: MethodDescription) extends AnyVal {

    def asType(
      implicit context: SparkClientCompiler.Context): Type = {
      Type.getType(md.resolve(context.jpContext.getClassLoader))
    }

    def name: String = md.getName

    def parameterClasses(
      implicit context: SparkClientCompiler.Context): Seq[Class[_]] = {
      md.getParameterTypes.map(_.resolve(context.jpContext.getClassLoader))
    }
  }

  implicit class AugmentedOperatorInput(val op: OperatorPort) extends AnyVal {

    def dataModelRef(
      implicit context: SparkClientCompiler.Context): DataModelReference = {
      context.jpContext.getDataModelLoader.load(op.getDataType)
    }

    def dataModelType(
      implicit context: SparkClientCompiler.Context): Type = {
      dataModelRef.getDeclaration.asType
    }

    def dataModelClass(
      implicit context: SparkClientCompiler.Context): Class[_] = {
      dataModelRef.getDeclaration.resolve(context.jpContext.getClassLoader)
    }
  }

  implicit class AugmentedOperatorArgument(val oa: OperatorArgument) extends AnyVal {

    def asType: Type = oa.getValue.getValueType.asType

    def resolveClass(
      implicit context: SparkClientCompiler.Context): Class[_] = {
      oa.getValue.getValueType.getErasure.resolve(context.jpContext.getClassLoader)
    }

    def value(
      implicit context: SparkClientCompiler.Context): Any = {
      oa.getValue.value
    }
  }

  implicit class AugmentedValueDescription(val vd: ValueDescription) extends AnyVal {

    def value(
      implicit context: SparkClientCompiler.Context): Any = {
      vd.resolve(context.jpContext.getClassLoader)
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
}
