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
import com.asakusafw.vocabulary.operator.Project

class ProjectOperatorCompiler extends CoreOperatorCompiler {

  override def support(operator: CoreOperator)(implicit context: Context): Boolean = {
    operator.getCoreOperatorKind == CoreOperatorKind.PROJECT
  }

  override def operatorType: OperatorType = OperatorType.MapType

  override def compile(operator: CoreOperator)(implicit context: Context): Type = {
    assert(support(operator))

    val operatorInfo = new OperatorInfo(operator)(context.jpContext)

    assert(operatorInfo.inputs.size == 1)
    assert(operatorInfo.outputs.size == 1)

    val builder = new CoreOperatorFragmentClassBuilder(
      context.flowId,
      operatorInfo.inputDataModelTypes(Project.ID_INPUT),
      operatorInfo.outputDataModelTypes(Project.ID_OUTPUT)) {

      override def defAddMethod(mb: MethodBuilder, dataModelVar: Var): Unit = {
        import mb._
        operatorInfo.outputDataModelRefs(Project.ID_INPUT).getProperties.foreach { property =>
          val p = operatorInfo.inputDataModelRefs(Project.ID_INPUT).findProperty(property.getName)
          assert(p != null)
          assert(p.getType == property.getType)
          val methodName = p.getDeclaration.getName
          val propertyType = p.getType.asType
          thisVar.push().getField("childDataModel", operatorInfo.outputDataModelTypes(Project.ID_OUTPUT))
            .invokeV(methodName, propertyType)
            .invokeV("copyFrom",
              dataModelVar.push().invokeV(methodName, propertyType))
        }
        thisVar.push().getField("child", classOf[Fragment[_]].asType)
          .invokeV("add", thisVar.push()
            .getField("childDataModel", operatorInfo.outputDataModelTypes(Project.ID_OUTPUT))
            .asType(classOf[AnyRef].asType))
        `return`()
      }
    }

    context.jpContext.addClass(builder)
  }
}
