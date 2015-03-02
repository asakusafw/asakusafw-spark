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
import com.asakusafw.vocabulary.operator.Extract

class ExtractOperatorCompiler extends UserOperatorCompiler {

  override def of: Class[_] = classOf[Extract]

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
    assert(outputs.size > 0)
    val outputDataModelRefs = outputs.map(output => context.jpContext.getDataModelLoader.load(output.getDataType))
    val outputDataModelTypes = outputDataModelRefs.map(_.getDeclaration.asType)

    val arguments = operator.getArguments.toSeq

    assert(methodType.getArgumentTypes.toSeq ==
      inputDataModelType
      +: outputDataModelTypes.map(_ => classOf[Result[_]].asType)
      ++: arguments.map(_.getValue.getValueType.asType))

    val builder = new FragmentClassBuilder(inputDataModelType) with OperatorField with OutputFragments {

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
            initOperatorField(mb)
            initOutputFields(mb, thisVar.nextLocal)
          }
      }

      override def defMethods(methodDef: MethodDef): Unit = {
        super.defMethods(methodDef)

        methodDef.newMethod("add", Seq(dataModelType)) { mb =>
          import mb._
          val resultVar = `var`(dataModelType, thisVar.nextLocal)
          getOperatorField(mb)
            .invokeV(
              methodDesc.getName,
              resultVar.push()
                +: outputs.map { output =>
                  getOutputField(mb, output).asType(classOf[Result[_]].asType)
                }
                ++: arguments.map { argument =>
                  ldc(argument.getValue.resolve(context.jpContext.getClassLoader))(
                    ClassTag(argument.getValue.getValueType.resolve(context.jpContext.getClassLoader)))
                }: _*)
          `return`()
        }

        methodDef.newMethod("add", Seq(classOf[AnyRef].asType)) { mb =>
          import mb._
          val resultVar = `var`(classOf[AnyRef].asType, thisVar.nextLocal)
          thisVar.push().invokeV("add", resultVar.push().cast(dataModelType))
          `return`()
        }

        methodDef.newMethod("reset", Seq.empty) { mb =>
          import mb._
          resetOutputs(mb)
          `return`()
        }
      }
    }

    context.jpContext.addClass(builder)
  }
}
