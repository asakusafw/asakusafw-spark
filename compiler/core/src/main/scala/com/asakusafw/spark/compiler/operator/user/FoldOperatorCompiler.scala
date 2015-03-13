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
import com.asakusafw.spark.runtime.fragment.{ CoGroupFragment, Fragment }
import com.asakusafw.spark.tools.asm._
import com.asakusafw.vocabulary.operator.Fold

class FoldOperatorCompiler extends UserOperatorCompiler {

  override def of: Class[_] = classOf[Fold]

  override def compile(operator: UserOperator)(implicit context: Context): Type = {
    val annotationDesc = operator.getAnnotation
    assert(annotationDesc.getDeclaringClass.resolve(context.jpContext.getClassLoader) == of)
    val methodDesc = operator.getMethod
    val methodType = Type.getType(methodDesc.resolve(context.jpContext.getClassLoader))
    val implementationClassType = operator.getImplementationClass.asType

    val inputs = operator.getInputs.toSeq
    assert(inputs.size == 1)
    val input = inputs.head
    val inputDataModelRef = context.jpContext.getDataModelLoader.load(input.getDataType)
    val inputDataModelType = inputDataModelRef.getDeclaration.asType

    val outputs = operator.getOutputs.toSeq
    assert(outputs.size == 1)
    val output = outputs.head
    val outputDataModelRef = context.jpContext.getDataModelLoader.load(output.getDataType)
    val outputDataModelType = outputDataModelRef.getDeclaration.asType

    assert(inputDataModelType == outputDataModelType)

    val arguments = operator.getArguments.toSeq

    assert(methodType.getArgumentTypes.toSeq ==
      Seq(inputDataModelType, inputDataModelType)
      ++ arguments.map(_.getValue.getValueType.asType))

    val builder = new CoGroupFragmentClassBuilder(context.flowId) with OperatorField with OutputFragments {

      override def operatorType: Type = implementationClassType
      override def operatorOutputs: Seq[OperatorOutput] = outputs

      override def defFields(fieldDef: FieldDef): Unit = {
        defOperatorField(fieldDef)
        defOutputFields(fieldDef)
      }

      override def defConstructors(ctorDef: ConstructorDef): Unit = {
        ctorDef.newInit(Seq(classOf[Fragment[_]].asType),
          new MethodSignatureBuilder()
            .newParameterType {
              _.newClassType(classOf[Fragment[_]].asType) {
                _.newTypeArgument(SignatureVisitor.INSTANCEOF, outputDataModelType)
              }
            }
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
            val iterVar = groupsVar.push().invokeI("head", classOf[AnyRef].asType)
              .cast(classOf[Iterable[_]].asType)
              .invokeI("iterator", classOf[Iterator[_]].asType)
              .store(groupsVar.nextLocal)
            val accVar = iterVar.push().invokeI("next", classOf[AnyRef].asType)
              .cast(inputDataModelType)
              .store(iterVar.nextLocal)
            loop { ctrl =>
              iterVar.push().invokeI("hasNext", Type.BOOLEAN_TYPE).unlessTrue(ctrl.break())
              getOperatorField(mb).invokeV(
                methodDesc.getName,
                Seq(
                  accVar.push(),
                  iterVar.push().invokeI("next", classOf[AnyRef].asType)
                    .cast(inputDataModelType))
                  ++ arguments.map { argument =>
                    ldc(argument.getValue.resolve(context.jpContext.getClassLoader))(
                      ClassTag(argument.getValue.getValueType.resolve(context.jpContext.getClassLoader)))
                  }: _*)
            }
            getOutputField(mb, output)
              .invokeV("add", accVar.push().asType(classOf[AnyRef].asType))
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
