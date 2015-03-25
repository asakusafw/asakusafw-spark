package com.asakusafw.spark.compiler
package operator
package core

import scala.collection.JavaConversions._

import org.objectweb.asm.Type

import com.asakusafw.lang.compiler.model.graph.CoreOperator
import com.asakusafw.lang.compiler.model.graph.CoreOperator.CoreOperatorKind
import com.asakusafw.spark.compiler.spi.CoreOperatorCompiler
import com.asakusafw.spark.runtime.fragment.Fragment
import com.asakusafw.spark.tools.asm._
import com.asakusafw.spark.tools.asm.MethodBuilder._
import com.asakusafw.vocabulary.operator.Extend

class ExtendOperatorCompiler extends CoreOperatorCompiler {

  override def support(operator: CoreOperator)(implicit context: Context): Boolean = {
    operator.getCoreOperatorKind == CoreOperatorKind.EXTEND
  }

  override def operatorType: OperatorType = OperatorType.MapType

  override def compile(operator: CoreOperator)(implicit context: Context): Type = {
    assert(support(operator))

    val operatorInfo = new OperatorInfo(operator)(context.jpContext)

    assert(operatorInfo.inputs.size == 1)
    assert(operatorInfo.outputs.size == 1)

    val builder = new CoreOperatorFragmentClassBuilder(
      context.flowId,
      operatorInfo.inputDataModelTypes(Extend.ID_INPUT),
      operatorInfo.outputDataModelTypes(Extend.ID_OUTPUT)) {

      override def defAddMethod(mb: MethodBuilder, dataModelVar: Var): Unit = {
        import mb._

        // ensure that OutputDataModel has all the members of InputDataModel.
        operatorInfo.inputDataModelRefs(Extend.ID_INPUT).getProperties.foreach { sourceProperty =>
          val p = operatorInfo.outputDataModelRefs(Extend.ID_OUTPUT).findProperty(sourceProperty.getName)
          assert(p != null)
          assert(p.getType == sourceProperty.getType)
        }

        // set all the members of output, 
        // especially members not in OutputDataModel, to the initial values.
        thisVar.push().getField("childDataModel", operatorInfo.outputDataModelTypes(Extend.ID_OUTPUT))
          .invokeV("reset")

        // copy the members from input.
        operatorInfo.outputDataModelRefs(Extend.ID_OUTPUT).getProperties.foreach { destProperty =>
          val methodName = destProperty.getDeclaration.getName
          val propertyType = destProperty.getType.asType
          val p = operatorInfo.inputDataModelRefs(Extend.ID_INPUT).findProperty(destProperty.getName)
          if (p != null) {
            thisVar.push().getField("childDataModel", operatorInfo.outputDataModelTypes(Extend.ID_OUTPUT))
              .invokeV(methodName, propertyType)
              .invokeV("copyFrom",
                dataModelVar.push().invokeV(methodName, propertyType))
          }
        }

        thisVar.push().getField("child", classOf[Fragment[_]].asType)
          .invokeV("add", thisVar.push()
            .getField("childDataModel", operatorInfo.outputDataModelTypes(Extend.ID_OUTPUT))
            .asType(classOf[AnyRef].asType))
        `return`()
      }
    }

    context.jpContext.addClass(builder)
  }
}
