package com.asakusafw.spark.compiler
package operator
package user

import scala.collection.JavaConversions._
import scala.reflect.ClassTag

import org.objectweb.asm.Type
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.lang.compiler.model.description.ValueDescription
import com.asakusafw.lang.compiler.model.graph.{ OperatorOutput, UserOperator }
import com.asakusafw.runtime.core.Result
import com.asakusafw.spark.compiler.spi.UserOperatorCompiler
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.runtime.fragment.Fragment
import com.asakusafw.vocabulary.operator.Convert

class ConvertOperatorCompiler extends UserOperatorCompiler {

  override def of: Class[_] = classOf[Convert]

  override def compile(operator: UserOperator)(implicit context: Context): Type = {
    val annotationDesc = operator.getAnnotation
    assert(annotationDesc.getDeclaringClass.resolve(context.jpContext.getClassLoader) == of)
    val methodDesc = operator.getMethod
    val methodType = Type.getType(methodDesc.resolve(context.jpContext.getClassLoader))
    val implementationClassType = operator.getImplementationClass.asType

    val inputs = operator.getInputs.toSeq
    assert(inputs.size == 1) // FIXME to take multiple inputs for side data?
    val input = inputs.head
    val inputDataModelRef = context.jpContext.getDataModelLoader.load(input.getDataType)
    val inputDataModelType = inputDataModelRef.getDeclaration.asType

    val outputs = operator.getOutputs.toSeq
    assert(outputs.size == 2)

    val originalOutput = outputs(Convert.ID_OUTPUT_ORIGINAL)
    val originalOutputDataModelRef = context.jpContext.getDataModelLoader.load(originalOutput.getDataType)
    val originalOutputDataModelType = originalOutputDataModelRef.getDeclaration.asType

    val convertedOutput = outputs(Convert.ID_OUTPUT_CONVERTED)
    val convertedOutputDataModelRef = context.jpContext.getDataModelLoader.load(convertedOutput.getDataType)
    val convertedOutputDataModelType = convertedOutputDataModelRef.getDeclaration.asType

    val arguments = operator.getArguments.toSeq

    assert(methodType.getArgumentTypes.toSeq ==
      inputDataModelType
      +: arguments.map(_.getValue.getValueType.asType))

    val builder = new FragmentClassBuilder(
      context.flowId,
      inputDataModelType) with OperatorField with OutputFragments {

      override val operatorType: Type = implementationClassType
      override def operatorOutputs: Seq[OperatorOutput] = outputs

      override def defFields(fieldDef: FieldDef): Unit = {
        defOperatorField(fieldDef)
        defOutputFields(fieldDef)
      }

      override def defConstructors(ctorDef: ConstructorDef): Unit = {
        ctorDef.newInit((0 until outputs.size).map(_ => classOf[Fragment[_]].asType),
          ((new MethodSignatureBuilder() /: outputs) {
            case (builder, output) =>
              builder.newParameterType {
                _.newClassType(classOf[Fragment[_]].asType) {
                  _.newTypeArgument(SignatureVisitor.INSTANCEOF, output.getDataType.asType)
                }
              }
          })
            .newVoidReturnType()
            .build()) { mb =>
            import mb._
            thisVar.push().invokeInit(superType)
            initOutputFields(mb, thisVar.nextLocal)
          }
      }

      override def defMethods(methodDef: MethodDef): Unit = {
        super.defMethods(methodDef)

        methodDef.newMethod("add", Seq(inputDataModelType)) { mb =>
          import mb._
          val resultVar = `var`(inputDataModelType, thisVar.nextLocal)
          getOutputField(mb, originalOutput)
            .invokeV("add", resultVar.push().asType(classOf[AnyRef].asType))
          getOutputField(mb, convertedOutput)
            .invokeV("add",
              getOperatorField(mb)
                .invokeV(
                  methodDesc.getName,
                  convertedOutputDataModelType,
                  resultVar.push()
                    +: arguments.map { argument =>
                      ldc(argument.getValue.resolve(context.jpContext.getClassLoader))(
                        ClassTag(argument.getValue.getValueType.resolve(context.jpContext.getClassLoader)))
                    }: _*)
                .asType(classOf[AnyRef].asType))
          `return`()
        }

        methodDef.newMethod("reset", Seq.empty) { mb =>
          import mb._
          resetOutputs(mb)
          `return`()
        }

        defGetOperator(methodDef)
      }
    }

    context.jpContext.addClass(builder)
  }
}
