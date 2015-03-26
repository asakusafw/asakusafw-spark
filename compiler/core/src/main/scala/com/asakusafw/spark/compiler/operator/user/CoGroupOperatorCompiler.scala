package com.asakusafw.spark.compiler
package operator
package user

import java.util.{ List => JList }

import scala.collection.JavaConversions
import scala.reflect.ClassTag

import org.objectweb.asm.Type

import com.asakusafw.lang.compiler.model.graph.UserOperator
import com.asakusafw.runtime.core.Result
import com.asakusafw.spark.compiler.spi.OperatorType
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._
import com.asakusafw.vocabulary.operator.{ CoGroup, GroupSort }

class CoGroupOperatorCompiler extends UserOperatorCompiler {

  override def support(operator: UserOperator)(implicit context: Context): Boolean = {
    val operatorInfo = new OperatorInfo(operator)(context.jpContext)
    import operatorInfo._
    annotationDesc.resolveClass == classOf[CoGroup] || annotationDesc.resolveClass == classOf[GroupSort]
  }

  override def operatorType: OperatorType = OperatorType.CoGroupType

  override def compile(operator: UserOperator)(implicit context: Context): Type = {
    assert(support(operator))

    val operatorInfo = new OperatorInfo(operator)(context.jpContext)
    import operatorInfo._

    assert(inputs.size > 0)

    assert(methodDesc.parameterTypes ==
      inputs.map(_ => classOf[JList[_]].asType)
      ++ outputs.map(_ => classOf[Result[_]].asType)
      ++ arguments.map(_.asType))

    val builder = new UserOperatorFragmentClassBuilder(
      context.flowId,
      classOf[Seq[Iterable[_]]].asType,
      implementationClassType,
      outputs) {

      override def defAddMethod(mb: MethodBuilder, dataModelVar: Var): Unit = {
        import mb._
        getOperatorField(mb)
          .invokeV(
            methodDesc.getName,
            inputs.zipWithIndex.map {
              case (_, i) =>
                getStatic(JavaConversions.getClass.asType, "MODULE$", JavaConversions.getClass.asType)
                  .invokeV("seqAsJavaList", classOf[JList[_]].asType,
                    dataModelVar.push()
                      .invokeI("apply", classOf[AnyRef].asType, ldc(i).box().asType(classOf[AnyRef].asType))
                      .cast(classOf[Iterable[_]].asType)
                      .invokeI("toSeq", classOf[Seq[_]].asType))
            }
              ++ outputs.map { output =>
                getOutputField(mb, output).asType(classOf[Result[_]].asType)
              }
              ++ arguments.map { argument =>
                ldc(argument.value)(ClassTag(argument.resolveClass))
              }: _*)
        `return`()
      }
    }

    context.jpContext.addClass(builder)
  }
}
