package com.asakusafw.spark.compiler
package operator
package user

import scala.reflect.ClassTag

import org.objectweb.asm.Type

import com.asakusafw.lang.compiler.model.graph.UserOperator
import com.asakusafw.spark.compiler.spi.OperatorType
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._
import com.asakusafw.vocabulary.operator.Branch

class BranchOperatorCompiler extends UserOperatorCompiler {

  override def support(operator: UserOperator)(implicit context: Context): Boolean = {
    val operatorInfo = new OperatorInfo(operator)(context.jpContext)
    import operatorInfo._
    annotationDesc.resolveClass == classOf[Branch]
  }

  override def operatorType: OperatorType = OperatorType.MapType

  override def compile(operator: UserOperator)(implicit context: Context): Type = {
    assert(support(operator))

    val operatorInfo = new OperatorInfo(operator)(context.jpContext)
    import operatorInfo._

    assert(inputs.size == 1) // FIXME to take multiple inputs for side data?
    assert(outputs.size > 0)

    outputs.foreach { output =>
      assert(output.dataModelType == inputs(Branch.ID_INPUT).dataModelType)
    }

    assert(methodDesc.parameterTypes ==
      inputs(Branch.ID_INPUT).dataModelType +: arguments.map(_.asType))

    val builder = new UserOperatorFragmentClassBuilder(
      context.flowId,
      inputs(Branch.ID_INPUT).dataModelType,
      implementationClassType,
      outputs) {

      override def defAddMethod(mb: MethodBuilder, dataModelVar: Var): Unit = {
        import mb._
        val branch = getOperatorField(mb)
          .invokeV(
            methodDesc.name,
            methodDesc.returnType,
            dataModelVar.push()
              +: arguments.map { argument =>
                ldc(argument.value)(ClassTag(argument.resolveClass))
              }: _*)
        branch.dup().unlessNotNull {
          `throw`(pushNew0(classOf[NullPointerException].asType))
        }
        branchOutputMap.foreach {
          case (output, enum) =>
            branch.dup().unlessNe(
              getStatic(methodDesc.returnType, enum.name, methodDesc.returnType)) {
                getOutputField(mb, output)
                  .invokeV("add", dataModelVar.push().asType(classOf[AnyRef].asType))
                `return`()
              }
        }
        `throw`(pushNew0(classOf[AssertionError].asType))
      }
    }

    context.jpContext.addClass(builder)
  }
}
