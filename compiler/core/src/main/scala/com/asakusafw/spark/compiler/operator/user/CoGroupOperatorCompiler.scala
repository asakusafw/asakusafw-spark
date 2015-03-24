package com.asakusafw.spark.compiler
package operator
package user

import java.util.{ List => JList }

import scala.collection.JavaConversions
import scala.reflect.ClassTag

import org.objectweb.asm.Type

import com.asakusafw.lang.compiler.model.graph.UserOperator
import com.asakusafw.runtime.core.Result
import com.asakusafw.spark.compiler.spi.UserOperatorCompiler
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._
import com.asakusafw.vocabulary.operator.CoGroup

class CoGroupOperatorCompiler extends UserOperatorCompiler {

  override def of: Class[_] = classOf[CoGroup]

  override def compile(operator: UserOperator)(implicit context: Context): Type = {

    val operatorInfo = new OperatorInfo(operator)(context.jpContext)

    assert(operatorInfo.inputs.size > 0)

    assert(operatorInfo.annotationDesc.getDeclaringClass.resolve(context.jpContext.getClassLoader) == of)
    assert(operatorInfo.methodType.getArgumentTypes.toSeq ==
      operatorInfo.inputDataModelTypes.map(_ => classOf[JList[_]].asType)
      ++ operatorInfo.outputDataModelTypes.map(_ => classOf[Result[_]].asType)
      ++ operatorInfo.argumentTypes)

    val builder = new UserOperatorFragmentClassBuilder(
      context.flowId,
      classOf[Seq[Iterable[_]]].asType,
      operatorInfo.implementationClassType,
      operatorInfo.outputs) {

      override def defAddMethod(mb: MethodBuilder, dataModelVar: Var): Unit = {
        import mb._
        getOperatorField(mb)
          .invokeV(
            operatorInfo.methodDesc.getName,
            operatorInfo.inputs.zipWithIndex.map {
              case (_, i) =>
                getStatic(JavaConversions.getClass.asType, "MODULE$", JavaConversions.getClass.asType)
                  .invokeV("seqAsJavaList", classOf[JList[_]].asType,
                    dataModelVar.push()
                      .invokeI("apply", classOf[AnyRef].asType, ldc(i).box().asType(classOf[AnyRef].asType))
                      .cast(classOf[Iterable[_]].asType)
                      .invokeI("toSeq", classOf[Seq[_]].asType))
            }
              ++ operatorInfo.outputs.map { output =>
                getOutputField(mb, output).asType(classOf[Result[_]].asType)
              }
              ++ operatorInfo.arguments.map { argument =>
                ldc(argument.getValue.resolve(context.jpContext.getClassLoader))(
                  ClassTag(argument.getValue.getValueType.resolve(context.jpContext.getClassLoader)))
              }: _*)
        `return`()
      }
    }

    context.jpContext.addClass(builder)
  }
}
