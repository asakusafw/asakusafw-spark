package com.asakusafw.spark.compiler
package operator
package user

import java.util.{ List => JList }

import scala.collection.JavaConversions
import scala.collection.JavaConversions._
import scala.reflect.ClassTag

import org.objectweb.asm.Type
import org.objectweb.asm.signature.SignatureVisitor

import com.asakusafw.lang.compiler.model.graph.{ OperatorOutput, UserOperator }
import com.asakusafw.runtime.core.Result
import com.asakusafw.spark.compiler.spi.UserOperatorCompiler
import com.asakusafw.spark.runtime.fragment.Fragment
import com.asakusafw.spark.tools.asm._
import com.asakusafw.vocabulary.operator.CoGroup

class CoGroupOperatorCompiler extends UserOperatorCompiler {

  override def of: Class[_] = classOf[CoGroup]

  override def compile(operator: UserOperator)(implicit context: Context): Type = {
    val annotationDesc = operator.getAnnotation
    assert(annotationDesc.getDeclaringClass.resolve(context.jpContext.getClassLoader) == of)
    val methodDesc = operator.getMethod
    val methodType = Type.getType(methodDesc.resolve(context.jpContext.getClassLoader))
    val implementationClassType = operator.getImplementationClass.asType

    val inputs = operator.getInputs.toSeq
    assert(inputs.size > 0)
    val inputDataModelRefs = inputs.map(input => context.jpContext.getDataModelLoader.load(input.getDataType))
    val inputDataModelTypes = inputDataModelRefs.map(_.getDeclaration.asType)

    val outputs = operator.getOutputs.toSeq
    assert(outputs.size > 0)
    val outputDataModelRefs = outputs.map(output => context.jpContext.getDataModelLoader.load(output.getDataType))
    val outputDataModelTypes = outputDataModelRefs.map(_.getDeclaration.asType)

    val arguments = operator.getArguments.toSeq

    assert(methodType.getArgumentTypes.toSeq ==
      inputDataModelTypes.map(_ => classOf[JList[_]].asType)
      ++ outputDataModelTypes.map(_ => classOf[Result[_]].asType)
      ++ arguments.map(_.getValue.getValueType.asType))

    val builder = new CoGroupFragmentClassBuilder with OperatorField with OutputFragments {

      override def operatorType: Type = implementationClassType
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

        methodDef.newMethod("add", Seq(classOf[Seq[Iterable[_]]].asType),
          new MethodSignatureBuilder()
            .newParameterType {
              _.newClassType(classOf[Seq[_]].asType) {
                _.newTypeArgument(SignatureVisitor.INSTANCEOF) {
                  _.newClassType(classOf[Iterable[_]].asType) {
                    _.newTypeArgument(SignatureVisitor.INSTANCEOF, classOf[AnyRef].asType)
                  }
                }
              }
            }
            .newVoidReturnType()
            .build()) { mb =>
            import mb._
            val groupsVar = `var`(classOf[Seq[Iterable[_]]].asType, thisVar.nextLocal)
            getOperatorField(mb)
              .invokeV(methodDesc.getName,
                (inputs.zipWithIndex.map {
                  case (input, i) =>
                    getStatic(JavaConversions.getClass.asType, "MODULE$", JavaConversions.getClass.asType)
                      .invokeV("seqAsJavaList", classOf[JList[_]].asType,
                        groupsVar.push()
                          .invokeI("apply", classOf[AnyRef].asType, ldc(i).box().asType(classOf[AnyRef].asType))
                          .cast(classOf[Iterable[_]].asType)
                          .invokeI("toSeq", classOf[Seq[_]].asType))
                })
                  ++ (outputs.map { output =>
                    getOutputField(mb, output).asType(classOf[Result[_]].asType)
                  })
                  ++ (arguments.map { argument =>
                    ldc(argument.getValue.resolve(context.jpContext.getClassLoader))(
                      ClassTag(argument.getValue.getValueType.resolve(context.jpContext.getClassLoader)))
                  }): _*)
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
