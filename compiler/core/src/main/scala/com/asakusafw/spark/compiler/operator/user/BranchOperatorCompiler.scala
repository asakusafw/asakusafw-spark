package com.asakusafw.spark.compiler
package operator
package user

import scala.collection.JavaConversions._
import scala.reflect.ClassTag

import org.objectweb.asm.Type

import com.asakusafw.lang.compiler.analyzer.util.BranchOperatorUtil
import com.asakusafw.lang.compiler.model.graph.UserOperator
import com.asakusafw.spark.compiler.spi.UserOperatorCompiler
import com.asakusafw.spark.runtime.fragment.Fragment
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._
import com.asakusafw.vocabulary.operator.Branch

class BranchOperatorCompiler extends UserOperatorCompiler {

  override def support(operator: UserOperator)(implicit context: Context): Boolean = {
    val operatorInfo = new OperatorInfo(operator)(context.jpContext)
    operatorInfo.annotationClass == classOf[Branch]
  }

  override def operatorType: OperatorType = OperatorType.MapType

  override def compile(operator: UserOperator)(implicit context: Context): Type = {
    assert(support(operator))

    val operatorInfo = new OperatorInfo(operator)(context.jpContext)

    assert(operatorInfo.inputs.size == 1) // FIXME to take multiple inputs for side data?
    assert(operatorInfo.outputs.size > 0)

    operatorInfo.outputDataModelTypes.foreach(outputDataModelType =>
      assert(outputDataModelType == operatorInfo.inputDataModelTypes(Branch.ID_INPUT)))

    assert(operatorInfo.methodType.getArgumentTypes.toSeq ==
      operatorInfo.inputDataModelTypes(Branch.ID_INPUT) +: operatorInfo.argumentTypes)

    val builder = new UserOperatorFragmentClassBuilder(
      context.flowId,
      operatorInfo.inputDataModelTypes(Branch.ID_INPUT),
      operatorInfo.implementationClassType,
      operatorInfo.outputs) {

      override def defAddMethod(mb: MethodBuilder, dataModelVar: Var): Unit = {
        import mb._
        val branch = getOperatorField(mb)
          .invokeV(
            operatorInfo.methodDesc.getName,
            operatorInfo.methodType.getReturnType,
            dataModelVar.push()
              +: operatorInfo.arguments.map { argument =>
                ldc(argument.getValue.resolve(context.jpContext.getClassLoader))(
                  ClassTag(argument.getValue.getValueType.resolve(context.jpContext.getClassLoader)))
              }: _*)
        branch.dup().unlessNotNull {
          `throw`(pushNew0(classOf[NullPointerException].asType))
        }
        operatorInfo.branchOutputMap.foreach {
          case (output, enum) =>
            branch.dup().unlessNe(
              getStatic(operatorInfo.methodType.getReturnType, enum.name, operatorInfo.methodType.getReturnType)) {
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
